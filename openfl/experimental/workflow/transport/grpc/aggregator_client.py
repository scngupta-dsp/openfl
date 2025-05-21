# Copyright 2020-2024 Intel Corporation
# SPDX-License-Identifier: Apache-2.0


"""AggregatorGRPCClient module."""

import time
from logging import getLogger
from typing import Optional, Tuple

import grpc

from openfl.experimental.workflow.protocols import aggregator_pb2, aggregator_pb2_grpc
from openfl.experimental.workflow.transport.grpc.grpc_channel_options import channel_options
from openfl.experimental.workflow.transport.grpc.utils import reassemble_chunks, stream_large_object

import dill
      
class ConstantBackoff:
    """Constant Backoff policy."""

    def __init__(self, reconnect_interval, logger, uri):
        """Initialize Constant Backoff."""
        self.reconnect_interval = reconnect_interval
        self.logger = logger
        self.uri = uri

    def sleep(self):
        """Sleep for specified interval."""
        self.logger.info(f"Attempting to connect to aggregator at {self.uri}")
        time.sleep(self.reconnect_interval)


class RetryOnRpcErrorClientInterceptor(
    grpc.UnaryUnaryClientInterceptor, grpc.StreamUnaryClientInterceptor
):
    """Retry gRPC connection on failure."""

    def __init__(
        self,
        sleeping_policy,
        status_for_retry: Optional[Tuple[grpc.StatusCode]] = None,
    ):
        """Initialize function for gRPC retry."""
        self.sleeping_policy = sleeping_policy
        self.status_for_retry = status_for_retry

    def _intercept_call(self, continuation, client_call_details, request_or_iterator):
        """Intercept the call to the gRPC server."""
        while True:
            response = continuation(client_call_details, request_or_iterator)

            if isinstance(response, grpc.RpcError):
                # If status code is not in retryable status codes
                self.sleeping_policy.logger.info(f"Response code: {response.code()}")
                if self.status_for_retry and response.code() not in self.status_for_retry:
                    return response

                self.sleeping_policy.sleep()
            else:
                return response

    def intercept_unary_unary(self, continuation, client_call_details, request):
        """Wrap intercept call for unary->unary RPC."""
        return self._intercept_call(continuation, client_call_details, request)

    def intercept_stream_unary(self, continuation, client_call_details, request_iterator):
        """Wrap intercept call for stream->unary RPC."""
        return self._intercept_call(continuation, client_call_details, request_iterator)


def _atomic_connection(func):
    def wrapper(self, *args, **kwargs):
        self.reconnect()
        response = func(self, *args, **kwargs)
        self.disconnect()
        return response

    return wrapper


def _resend_data_on_reconnection(func):
    def wrapper(self, *args, **kwargs):
        while True:
            try:
                response = func(self, *args, **kwargs)
            except grpc.RpcError as e:
                if e.code() == grpc.StatusCode.UNKNOWN:
                    self.logger.info(
                        f"Attempting to resend data request to aggregator at {self.uri}"
                    )
                elif e.code() == grpc.StatusCode.UNAUTHENTICATED:
                    raise
                continue
            break
        return response

    return wrapper


class AggregatorGRPCClient:
    """Client to the aggregator over gRPC-TLS."""

    def __init__(
        self,
        agg_addr,
        agg_port,
        disable_client_auth,
        root_certificate,
        certificate,
        private_key,
        tls=True,
        aggregator_uuid=None,
        federation_uuid=None,
        single_col_cert_common_name=None,
        **kwargs,
    ):
        """Initialize."""
        self.uri = f"{agg_addr}:{agg_port}"
        self.tls = tls
        self.disable_client_auth = disable_client_auth
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key

        self.logger = getLogger(__name__)

        if not self.tls:
            self.logger.warn("gRPC is running on insecure channel with TLS disabled.")
            self.channel = self.create_insecure_channel(self.uri)
        else:
            self.channel = self.create_tls_channel(
                self.uri,
                self.root_certificate,
                self.disable_client_auth,
                self.certificate,
                self.private_key,
            )

        self.header = None
        self.aggregator_uuid = aggregator_uuid
        self.federation_uuid = federation_uuid
        self.single_col_cert_common_name = single_col_cert_common_name

        # Adding an interceptor for RPC Errors
        self.interceptors = (
            RetryOnRpcErrorClientInterceptor(
                sleeping_policy=ConstantBackoff(
                    logger=self.logger,
                    reconnect_interval=int(kwargs.get("client_reconnect_interval", 1)),
                    uri=self.uri,
                ),
                status_for_retry=(grpc.StatusCode.UNAVAILABLE,),
            ),
        )
        self.stub = aggregator_pb2_grpc.AggregatorStub(
            grpc.intercept_channel(self.channel, *self.interceptors)
        )

    def create_insecure_channel(self, uri):
        """Set an insecure gRPC channel (i.e. no TLS) if desired.

        Warns user that this is not recommended.

        Args:
            uri: The uniform resource identifier of the insecure channel

        Returns:
            An insecure gRPC channel object
        """
        return grpc.insecure_channel(uri, options=channel_options)

    def create_tls_channel(
        self,
        uri,
        root_certificate,
        disable_client_auth,
        certificate,
        private_key,
    ):
        """Set an secure gRPC channel (i.e. TLS).

        Args:
            uri: The uniform resource identifier of the insecure channel
            root_certificate: The Certificate Authority filename
            disable_client_auth (boolean): True disabled client-side
             authentication (not recommended, throws warning to user)
            certificate: The client certificate filename from the collaborator
             (signed by the certificate authority)

        Returns:
            An insecure gRPC channel object
        """
        with open(root_certificate, "rb") as f:
            root_certificate_b = f.read()

        if disable_client_auth:
            self.logger.warn("Client-side authentication is disabled.")
            private_key_b = None
            certificate_b = None
        else:
            with open(private_key, "rb") as f:
                private_key_b = f.read()
            with open(certificate, "rb") as f:
                certificate_b = f.read()

        credentials = grpc.ssl_channel_credentials(
            root_certificates=root_certificate_b,
            private_key=private_key_b,
            certificate_chain=certificate_b,
        )

        return grpc.secure_channel(uri, credentials, options=channel_options)

    def _set_header(self, collaborator_name):
        self.header = aggregator_pb2.MessageHeader(
            sender=collaborator_name,
            receiver=self.aggregator_uuid,
            federation_uuid=self.federation_uuid,
            single_col_cert_common_name=self.single_col_cert_common_name or "",
        )

    def validate_response(self, reply, collaborator_name):
        """Validate the aggregator response."""
        assert reply.header.receiver == collaborator_name, (
            f"Receiver in response header does not match collaborator name. "
            f"Expected: {collaborator_name}, Actual: {reply.header.receiver}"
        )
        assert reply.header.sender == self.aggregator_uuid, (
            f"Sender in response header does not match aggregator UUID. "
            f"Expected: {self.aggregator_uuid}, Actual: {reply.header.sender}"
        )
        assert reply.header.federation_uuid == self.federation_uuid, (
            f"Federation UUID in response header does not match. "
            f"Expected: {self.federation_uuid}, Actual: {reply.header.federation_uuid}"
        )
        assert reply.header.single_col_cert_common_name == (
            self.single_col_cert_common_name or ""
        ), (
            f"Single collaborator certificate common name in response header does not match. "
            f"Expected: {self.single_col_cert_common_name or ''}, Actual: {reply.header.single_col_cert_common_name}"  # noqa: E501
        )

    def disconnect(self):
        """Close the gRPC channel."""
        self.logger.debug(f"Disconnecting from gRPC server at {self.uri}")
        self.channel.close()

    def reconnect(self):
        """Create a new channel with the gRPC server."""
        # channel.close() is idempotent. Call again here in case it wasn't
        # issued previously
        self.disconnect()

        if not self.tls:
            self.channel = self.create_insecure_channel(self.uri)
        else:
            self.channel = self.create_tls_channel(
                self.uri,
                self.root_certificate,
                self.disable_client_auth,
                self.certificate,
                self.private_key,
            )

        self.logger.debug(f"Connecting to gRPC at {self.uri}")

        self.stub = aggregator_pb2_grpc.AggregatorStub(
            grpc.intercept_channel(self.channel, *self.interceptors)
        )

    @_atomic_connection
    @_resend_data_on_reconnection
    def send_task_results(self, collaborator_name, round_number, next_step, clone_bytes):
        """Send next function name to aggregator."""
        self._set_header(collaborator_name)

        request_metadata = aggregator_pb2.TaskResultsRequest(
                    header=self.header,
                    collab_name=collaborator_name,
                    round_number=round_number,
                    next_step=next_step,
                    execution_environment=b''  # Placeholder for the large object
                )

        # Stream the request metadata and serialized large object in chunks
        request_iterator = stream_large_object(request_metadata, [clone_bytes], aggregator_pb2.Chunk)
        response = self.stub.SendTaskResults(request_iterator)

        self.validate_response(response, collaborator_name)

        return response.header
 
    @_atomic_connection
    @_resend_data_on_reconnection
    def get_tasks(self, collaborator_name):
        """Get tasks from the aggregator."""
        self._set_header(collaborator_name)
        request = aggregator_pb2.GetTasksRequest(header=self.header)

        # response = self.stub.GetTasks(request)
        response_stream = self.stub.GetTasks(request)
        response, execution_environment = reassemble_chunks(response_stream, aggregator_pb2.GetTasksResponse)        

        self.validate_response(response, collaborator_name)

        if not execution_environment:
            execution_environment = None

        return (
            response.round_number,
            response.function_name,
            execution_environment,
            response.sleep_time,
            response.quit,
        )

    @_atomic_connection
    @_resend_data_on_reconnection
    def call_checkpoint(self, collaborator_name, clone_bytes, function, stream_buffer):
        """Perform checkpoint for collaborator task."""
        self._set_header(collaborator_name)

        print(f"****** Checkpoint called from: {collaborator_name}")
        print(f"execution_environment_size={len(clone_bytes)/(1024*1024)} MB, function_size={len(function)/(1024*1024)} MB, stream_buffer_size={len(stream_buffer)/(1024*1024)} MB")
        print(f"stream_buffer contents: {dill.loads(stream_buffer)}")

        # Create the initial request metadata with sizes
        request_metadata = aggregator_pb2.CheckpointRequest(
            header=self.header,
            execution_environment=b'',  # Placeholder
            function=b'',               # Placeholder
            stream_buffer=b'',          # Placeholder
            execution_environment_size=len(clone_bytes),
            function_size=len(function),
            stream_buffer_size=len(stream_buffer)
        )

        # Create the request iterator that streams the large objects
        large_objects = [clone_bytes, function, stream_buffer]
        request_iterator = stream_large_object(request_metadata, large_objects, aggregator_pb2.Chunk)
        response = self.stub.CallCheckpoint(request_iterator)
        self.validate_response(response, collaborator_name)

        return response.header
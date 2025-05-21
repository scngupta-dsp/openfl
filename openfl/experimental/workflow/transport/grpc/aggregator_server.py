# Copyright 2020-2024 Intel Corporation
# SPDX-License-Identifier: Apache-2.0


"""AggregatorGRPCServer module."""

import logging
from concurrent.futures import ThreadPoolExecutor
from multiprocessing import cpu_count
from random import random
from time import sleep

from grpc import StatusCode, server, ssl_server_credentials

from openfl.experimental.workflow.protocols import aggregator_pb2, aggregator_pb2_grpc
from openfl.experimental.workflow.transport.grpc.grpc_channel_options import channel_options
from openfl.experimental.workflow.transport.grpc.utils import reassemble_chunks, stream_large_object

logger = logging.getLogger(__name__)

def reassemble_chunks_call_checkpoint(request_iterator):
    """Reassemble the chunks from the request iterator.

    Args:
        request_iterator: The iterator of streamed chunks.

    Returns:
        Tuple containing request metadata, execution environment, function, and stream buffer.
    """
    request_data = b''
    execution_environment = b''
    function = b''
    stream_buffer = b''

    is_metadata_processed = False

    # Process the incoming chunks
    chunk_index = 0
    for chunk in request_iterator:
        if not is_metadata_processed:
            request_metadata = aggregator_pb2.CheckpointRequest()
            request_metadata.ParseFromString(chunk.chunk)
            is_metadata_processed = True
        else:
            chunk_index = chunk_index + 1
            if (chunk_index % 128 == 0):
                print(f"****** Received chunk index: {chunk_index}")
            request_data += chunk.chunk

    # Use the sizes from the metadata to split the byte stream
    execution_environment_size = request_metadata.execution_environment_size
    function_size = request_metadata.function_size
    stream_buffer_size = request_metadata.stream_buffer_size
    print(f"Received: execution_environment_size = {execution_environment_size}, function_size = {function_size}, stream_buffer_size = {stream_buffer_size}")

    execution_environment = request_data[:execution_environment_size]
    function = request_data[execution_environment_size:execution_environment_size + function_size]
    stream_buffer = request_data[execution_environment_size + function_size:execution_environment_size + function_size + stream_buffer_size]

    return request_metadata, execution_environment, function, stream_buffer

class AggregatorGRPCServer(aggregator_pb2_grpc.AggregatorServicer):
    """GRPC server class for the Aggregator."""

    def __init__(
        self,
        aggregator,
        agg_port,
        tls=True,
        disable_client_auth=False,
        root_certificate=None,
        certificate=None,
        private_key=None,
        **kwargs,
    ):
        """Class initializer.

        Args:
            aggregator: The aggregator
        Args:
            fltask (FLtask): The gRPC service task.
            tls (bool): To disable the TLS. (Default: True)
            disable_client_auth (bool): To disable the client side
            authentication. (Default: False)
            root_certificate (str): File path to the CA certificate.
            certificate (str): File path to the server certificate.
            private_key (str): File path to the private key.
            kwargs (dict): Additional arguments to pass into function
        """
        self.aggregator = aggregator
        self.uri = f"[::]:{agg_port}"
        self.tls = tls
        self.disable_client_auth = disable_client_auth
        self.root_certificate = root_certificate
        self.certificate = certificate
        self.private_key = private_key
        self.server = None
        self.server_credentials = None

        self.logger = logging.getLogger(__name__)

    def validate_collaborator(self, request, context):
        """Validate the collaborator.

        Args:
            request: The gRPC message request
            context: The gRPC context

        Raises:
            ValueError: If the collaborator or collaborator certificate is not
                valid then raises error.
        """
        if self.tls:
            common_name = context.auth_context()["x509_common_name"][0].decode("utf-8")
            collaborator_common_name = request.header.sender
            if not self.aggregator.valid_collaborator_cn_and_id(
                common_name, collaborator_common_name
            ):
                # Random delay in authentication failures
                sleep(5 * random())
                context.abort(
                    StatusCode.UNAUTHENTICATED,
                    f"Invalid collaborator. CN: |{common_name}| "
                    f"collaborator_common_name: |{collaborator_common_name}|",
                )

    def get_header(self, collaborator_name):
        """Compose and return MessageHeader.

        Args:
            collaborator_name : str
                The collaborator the message is intended for
        """
        return aggregator_pb2.MessageHeader(
            sender=self.aggregator.uuid,
            receiver=collaborator_name,
            federation_uuid=self.aggregator.federation_uuid,
            single_col_cert_common_name=self.aggregator.single_col_cert_common_name,
        )

    def check_request(self, request):
        """Validate request header matches expected values.

        Args:
            request : protobuf
                Request sent from a collaborator that requires validation
        """
        assert request.header.sender in self.aggregator.authorized_cols, (
            f"Sender in request header is not authorized. "
            f"Expected: one of {self.aggregator.authorized_cols}, Actual: {request.header.sender}"
        )

        assert request.header.receiver == self.aggregator.uuid, (
            f"Receiver in request header does not match aggregator UUID. "
            f"Expected: {self.aggregator.uuid}, Actual: {request.header.receiver}"
        )

        assert request.header.federation_uuid == self.aggregator.federation_uuid, (
            f"Federation UUID in request header does not match. "
            f"Expected: {self.aggregator.federation_uuid}, Actual: {request.header.federation_uuid}"
        )

        assert (
            request.header.single_col_cert_common_name
            == self.aggregator.single_col_cert_common_name
        ), (
            f"Single collaborator certificate common name in request header does not match. "
            f"Expected: {self.aggregator.single_col_cert_common_name}, Actual: {request.header.single_col_cert_common_name}"  # noqa: E501
        )

    def SendTaskResults(self, request, context):  # NOQA:N802
        """<FIND OUT WHAT COMMENT TO PUT HERE>.

        Args:
            request: The gRPC message request
            context: The gRPC context
        """
        request_metadata, execution_environment = reassemble_chunks(request, aggregator_pb2.TaskResultsRequest)

        self.validate_collaborator(request_metadata, context)
        self.check_request(request_metadata)
        collaborator_name = request_metadata.header.sender
        round_number = (request_metadata.round_number,)
        next_step = (request_metadata.next_step,)

        _ = self.aggregator.send_task_results(
            collaborator_name, round_number[0], next_step, execution_environment
        )

        return aggregator_pb2.TaskResultsResponse(header=self.get_header(collaborator_name))

    def GetTasks(self, request, context):  # NOQA:N802
        """Request a job from aggregator.

        Args:
            request: The gRPC message request.
            context: The gRPC context.
        """
        self.validate_collaborator(request, context)
        self.check_request(request)
        collaborator_name = request.header.sender

        rn, f, ee, st, q = self.aggregator.get_tasks(request.header.sender)

        ee_bytes = ee if ee is not None else b''
            
        # Build the GetTasksResponse
        response = aggregator_pb2.GetTasksResponse(
            header=self.get_header(collaborator_name),
            round_number=rn,
            function_name=f,
            execution_environment=b'',
            sleep_time=st,
            quit=q
        )

        # Stream the response metadata and serialized large object in chunks
        return stream_large_object(response, [ee_bytes], aggregator_pb2.Chunk)

    def CallCheckpoint(self, request, context):  # NOQA:N802
        """Request aggregator to perform a checkpoint for a given function.

        Args:
            request: The gRPC message request
            context: The gRPC context
        """
        request_metadata, execution_environment, function, stream_buffer = reassemble_chunks_call_checkpoint(request)

        self.validate_collaborator(request_metadata, context)
        self.check_request(request_metadata)
        collaborator_name = request_metadata.header.sender
        # execution_environment = request.execution_environment
        # function = request_metadata.function
        # stream_buffer = request_metadata.stream_buffer

        self.aggregator.call_checkpoint(
            collaborator_name, execution_environment, function, stream_buffer
        )

        return aggregator_pb2.CheckpointResponse(header=self.get_header(collaborator_name))

    def get_server(self):
        """Return gRPC server."""
        self.server = server(ThreadPoolExecutor(max_workers=cpu_count()), options=channel_options)

        aggregator_pb2_grpc.add_AggregatorServicer_to_server(self, self.server)

        if not self.tls:
            self.logger.warn("gRPC is running on insecure channel with TLS disabled.")
            port = self.server.add_insecure_port(self.uri)
            self.logger.info(f"Insecure port: {port}")

        else:
            with open(self.private_key, "rb") as f:
                private_key_b = f.read()
            with open(self.certificate, "rb") as f:
                certificate_b = f.read()
            with open(self.root_certificate, "rb") as f:
                root_certificate_b = f.read()

            if self.disable_client_auth:
                self.logger.warn("Client-side authentication is disabled.")

            self.server_credentials = ssl_server_credentials(
                ((private_key_b, certificate_b),),
                root_certificates=root_certificate_b,
                require_client_auth=not self.disable_client_auth,
            )

            self.server.add_secure_port(self.uri, self.server_credentials)

        return self.server

import grpc
from concurrent import futures
import pickle
import aggregator_pb2
import aggregator_pb2_grpc
import numpy as np
from openfl.transport.grpc.common import DEFAULT_CHANNEL_OPTIONS
from model_utils import CustomClass

class AggregatorServicer(aggregator_pb2_grpc.AggregatorServicer):
    def GetTasks(self, request, context):
        # Create a large CustomClass object
        large_object = CustomClass(10*1024*1024*1024)
        serialized_large_object = pickle.dumps(large_object)

        # Build the GetTasksResponse
        response = aggregator_pb2.GetTasksResponse(
            header=request.header,
            round_number=1,
            function_name="example_function",
            ee=b'',  # Placeholder for the large object
            sleep_time=10,
            quit=False
        )

        # Send the GetTasksResponse metadata first
        yield aggregator_pb2.GetTasksResponseChunk(chunk=response.SerializeToString())

        # Split the serialized large object into chunks and stream them
        chunk_size = 2* 1024 * 1024  # 1 MB
        for i in range(0, len(serialized_large_object), chunk_size):
            chunk = serialized_large_object[i:i + chunk_size]
            yield aggregator_pb2.GetTasksResponseChunk(chunk=chunk)

def serve():
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.max_send_message_length', 1024 * 1024 * 50),  # 50 MB
            ('grpc.max_receive_message_length', 1024 * 1024 * 50)  # 50 MB
        ]
    )
    aggregator_pb2_grpc.add_AggregatorServicer_to_server(AggregatorServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started at [::]:50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()
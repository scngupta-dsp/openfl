import grpc
from concurrent import futures
import pickle
import aggregator_pb2
import aggregator_pb2_grpc
from model_utils import CustomClass
from openfl.protocols.utils import proto_to_datastream
from openfl.transport.grpc.common import DEFAULT_CHANNEL_OPTIONS

class AggregatorServicer(aggregator_pb2_grpc.AggregatorServicer):
    def GetTasks(self, request, context):
        # Create a large Python object
        large_object = CustomClass(array_size = 2*1024*1024*1024)
        serialized_large_object = pickle.dumps(large_object)

        # Build the GetTasksResponse
        response = aggregator_pb2.GetTasksResponse(
            header=request.header,
            round_number=1,
            function_name="example_function",
            ee=serialized_large_object,
            sleep_time=10,
            quit=False
        )

        # Split the response into chunks
        chunk_size = 1024 * 1024  # 1 MB
        serialized_response = response.SerializeToString()
        for i in range(0, len(serialized_response), chunk_size):
            chunk = serialized_response[i:i + chunk_size]
            yield aggregator_pb2.GetTasksResponseChunk(chunk=chunk)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10), options=DEFAULT_CHANNEL_OPTIONS)
    aggregator_pb2_grpc.add_AggregatorServicer_to_server(AggregatorServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    print("Server started at [::]:50051")
    server.wait_for_termination()

if __name__ == '__main__':
    serve()


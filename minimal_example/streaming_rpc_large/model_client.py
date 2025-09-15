import grpc
import pickle
import aggregator_pb2
import aggregator_pb2_grpc
from model_utils import CustomClass

def reassemble_chunks(chunk_stream):
    """Reassemble chunks from a stream into a single byte array."""
    npbytes = bytearray()
    response_metadata = None

    for chunk in chunk_stream:
        if response_metadata is None:
            response_metadata = aggregator_pb2.GetTasksResponse()
            response_metadata.ParseFromString(chunk.chunk)
        else:
            npbytes.extend(chunk.chunk)
    return response_metadata, bytes(npbytes)

def reassemble_chunks(chunk_stream):
    """Reassemble chunks from a stream into a single byte array."""
    npbytes = bytearray()
    response_metadata = None

    for chunk in chunk_stream:
        if response_metadata is None:
            response_metadata = aggregator_pb2.GetTasksResponse()
            response_metadata.ParseFromString(chunk.chunk)
        else:
            npbytes.extend(chunk.chunk)
    
    return response_metadata, bytes(npbytes)

def get_tasks(stub):
    # Create a request
    header = aggregator_pb2.MessageHeader(sender="client", receiver="server")
    request = aggregator_pb2.GetTasksRequest(header=header)

    # Get the response stream
    response_stream = stub.GetTasks(request)

    # Reassemble the chunks
    response_metadata, serialized_large_object = reassemble_chunks(response_stream)
    large_object = pickle.loads(serialized_large_object)

    # Use the deserialized object
    large_object.print_array_size()
    large_object.print_array_shape()

    print(f"Round Number: {response_metadata.round_number}")
    print(f"Function Name: {response_metadata.function_name}")
    print(f"Sleep Time: {response_metadata.sleep_time}")
    print(f"Quit: {response_metadata.quit}")

def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = aggregator_pb2_grpc.AggregatorStub(channel)
        get_tasks(stub)

if __name__ == '__main__':
    run()
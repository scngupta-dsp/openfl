import grpc
import pickle
import aggregator_pb2
import aggregator_pb2_grpc
from model_utils import CustomClass

def reassemble_chunks(chunk_stream):
    """Reassemble chunks from a stream into a single byte array."""
    npbytes = bytearray()
    index = 0
    for chunk in chunk_stream:
        index = index + 1
        npbytes.extend(chunk.chunk)
    print(f"<reassemble_chunks>: received: {index} chunks, total size: {len(npbytes)/(1024*1024)} MB")
    return bytes(npbytes)

def get_tasks(stub):
    # Create a request
    header = aggregator_pb2.MessageHeader(sender="client", receiver="server")
    request = aggregator_pb2.GetTasksRequest(header=header)

    # Get the response stream
    response_stream = stub.GetTasks(request)

    # Reassemble the chunks
    serialized_response = reassemble_chunks(response_stream)
    response = aggregator_pb2.GetTasksResponse()
    response.ParseFromString(serialized_response)

    # Deserialize the large object
    large_object = pickle.loads(response.ee)

    # Use the deserialized object
    large_object.print_array_size()
    large_object.print_array_shape()

    print(f"Round Number: {response.round_number}")
    print(f"Function Name: {response.function_name}")
    print(f"Sleep Time: {response.sleep_time}")
    print(f"Quit: {response.quit}")


def run():
    with grpc.insecure_channel('localhost:50051') as channel:
        stub = aggregator_pb2_grpc.AggregatorStub(channel)
        get_tasks(stub)

if __name__ == '__main__':
    run()

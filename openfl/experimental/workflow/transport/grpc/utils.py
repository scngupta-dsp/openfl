def reassemble_chunks(chunk_stream, message_type):
    """Reassemble chunks from a stream into a single byte array.

    Args:
        chunk_stream: The stream of chunks.
        message_type: The type of the message to reassemble.

    Returns:
        A tuple containing the reassembled message and the byte array.
    """
    npbytes = bytearray()
    message = None

    for chunk in chunk_stream:
        if message is None:
            message = message_type()
            message.ParseFromString(chunk.chunk)
        else:
            npbytes.extend(chunk.chunk)

    return message, bytes(npbytes) if npbytes else None

def stream_large_object(request_metadata, large_objects, message_type, chunk_size=16*1024*1024):
    """Stream the request metadata and serialized large objects in chunks.

    Args:
        request_metadata: The metadata message.
        large_objects: A list of large objects to stream.
        message_type: The message type to use for chunks.
        chunk_size: The size of each chunk.
    """
    yield message_type(chunk=request_metadata.SerializeToString())

    for large_object in large_objects:
        for i in range(0, len(large_object), chunk_size):
            yield message_type(chunk=large_object[i:i + chunk_size])

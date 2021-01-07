import erasure_coding
from utils import write_file
from asgiref.sync import async_to_sync, sync_to_async



def worker_encode(message_lead_node, send_task_socket_worker_encode, send_result_socket_lead_node,
                  response_socket_worker):
    data = message_lead_node[1]
    data = bytearray(data)
    max_erasures = int(bytes(message_lead_node[2]).decode())

    print("Encode - Sending data to storage")
    fragment_names = erasure_coding.encoding_file(data, max_erasures,
                                                  send_task_socket_worker_encode,
                                                  response_socket_worker)
    print("Encode - Response from Storage nodes")
    send_result_socket_lead_node.send_multipart(
        [bytes(fragment_names[0], 'utf-8'),
         bytes(fragment_names[1], 'utf-8'),
         bytes(fragment_names[2], 'utf-8'),
         bytes(fragment_names[3], 'utf-8')])
    print("Encode - Fragment Names sent to Lead Node")


def worker_decode(message_lead_node, send_task_socket_worker_decode, send_result_socket_lead_node,
                  response_socket_worker):
    data = message_lead_node
    fragment_names = []
    for i in range(3, 7):
        fragment_names.append(str(data[i], 'utf-8'))
    print(fragment_names)

    max_erasures = int(bytes(message_lead_node[1]).decode())
    file_size = int(bytes(message_lead_node[2]).decode())
    print(file_size)

    print("Decode - Sending data to storage")
    file_data = erasure_coding.decoding_file(fragment_names, max_erasures, file_size,
                                             send_task_socket_worker_decode,
                                             response_socket_worker)
    print("Decode - Response from Storage nodes")
    send_result_socket_lead_node.send_multipart([file_data])
    print("Decode - File Data sent to Lead Node")


def recv(recv, sender, messages_pb2, data_folder):
    # Incoming message on the 'receiver' socket where we get tasks to store a chunk
    msg = recv.recv_multipart()
    # Parse the Protobuf message from the first frame
    task = messages_pb2.storedata_request()
    task.ParseFromString(msg[0])
    # The data is the second frame
    data = msg[1]
    print('Chunk to save: %s, size: %d bytes' % (task.filename, len(data)))
    # Store the chunk with the given filename
    chunk_local_path = data_folder + '/' + task.filename
    write_file(data, chunk_local_path)
    print("Chunk saved to %s" % chunk_local_path)
    sender.send_string(task.filename)


def sub(subcriber, sender, messages_pb2, data_folder):
    # Incoming message on the 'subscriber' socket where we get retrieve requests
    msg = subcriber.recv()
    # Parse the Protobuf message from the first frame
    task = messages_pb2.getdata_request()
    task.ParseFromString(msg)
    filename = task.filename
    print("Data chunk request: %s" % filename)

    # Try to load the requested file from the local file system,
    # send response only if found
    try:
        with open(data_folder + '/' + filename, "rb") as in_file:
            print("Found chunk %s, sending it back" % filename)
            data = in_file.read()
            sender.send_multipart([bytes(filename, 'utf-8'), data])
    except FileNotFoundError:
        # This is OK here
        pass

import messages_pb2
import math
import random

from utils import random_string

def store_file(file_data, replicas, send_task_socket, response_socket):
    """
    Implements storing replications of a file.

    :param file_data: A bytearray that holds the file contents
    :param replicas: Number of replicas to be stored in different nodes
    :param send_task_socket: A ZMQ PUSH socket to the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The randomly generated file name
    """
    size = len(file_data)
    # Generate a random name for the replication
    file_data_name = random_string(8)
    print("Filename for replication is: %s" % file_data_name)

    for rep in range(int(replicas)):
        # Send 1 'store data' protobuf request with file name
        task = messages_pb2.storedata_request()
        task.filename = file_data_name
        send_task_socket.send_multipart([
            task.SerializeToString(),
            file_data
        ])

    # Wait until we receive 1 response from the workers
    for task_nbr in range(int(replicas)):
        resp = response_socket.recv_string()
        print('Received: %s' % resp)
    
    # Return the chunk names of each replica
    return file_data_name
#

def get_file(replication_filename, data_req_socket, response_socket):
    """
    Implements retrieving a file that is stored with Replication using 4 storage nodes.

    :param replication_filename: Name of file to retrieve 
    :param data_req_socket: A ZMQ SUB socket to request chunks from the storage nodes
    :param response_socket: A ZMQ PULL socket where the storage nodes respond.
    :return: The original file contents
    """

    # Request file
    task1 = messages_pb2.getdata_request()
    task1.filename = replication_filename
    data_req_socket.send(
        task1.SerializeToString()
    )

    # Receive the file 
    file_data = [None]
    result = response_socket.recv_multipart()
    filename_received = result[0].decode('utf-8')
    file_data = result[1]
    print("Received %s" % filename_received)
    return file_data
#
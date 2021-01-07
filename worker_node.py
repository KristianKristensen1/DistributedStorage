import zmq
import sys
import os
import helper_functions

data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        # OK, the folder exists
        pass
print("Data folder: %s" % data_folder)

context = zmq.Context()
leanode_ip = "192.168.0.100"#"localhost"

receiver_random_lead_node = context.socket(zmq.PULL)
receiver_random_lead_node.connect("tcp://"+leanode_ip+":5560")
print("Listening on " + "tcp://"+leanode_ip+":5560")

# Socket to send result to lead node
send_result_socket_lead_node = context.socket(zmq.PUSH)
send_result_socket_lead_node.connect("tcp://"+leanode_ip+":5563")

# Socket to send tasks to Storage Nodes
send_task_socket_worker_encode = context.socket(zmq.PUSH)
send_task_socket_worker_encode.bind("tcp://*:5561")

# Socket to receive messages from Storage Nodes
response_socket_worker = context.socket(zmq.PULL)
response_socket_worker.bind("tcp://*:5562")

# Socket to send tasks to Storage Nodes
send_task_socket_worker_decode = context.socket(zmq.PUB)
send_task_socket_worker_decode.bind("tcp://*:5564")

# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(receiver_random_lead_node, zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # Request to encode or decode from LEAD NODE
    if receiver_random_lead_node in socks:
        message_lead_node = receiver_random_lead_node.recv_multipart()
        encode_decode = bytes(message_lead_node[0]).decode()

        if encode_decode == 'encode':
            helper_functions.worker_encode(message_lead_node,
                                           send_task_socket_worker_encode,
                                           send_result_socket_lead_node,
                                           response_socket_worker)
        elif encode_decode == 'decode':
            helper_functions.worker_decode(message_lead_node,
                                           send_task_socket_worker_decode,
                                           send_result_socket_lead_node,
                                           response_socket_worker)

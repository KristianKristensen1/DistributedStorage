import zmq
import messages_pb2
import sys
import os
import helper_functions
from utils import random_string, write_file, is_raspberry_pi

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./' + data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

# if is_raspberry_pi():
#    # On the Raspberry Pi: ask the user to input the last segment of the server IP address
#    server_address = input("Server address: 192.168.0.___ ")
#    pull_address = "tcp://192.168.0." + server_address + ":5557"
#    sender_address = "tcp://192.168.0." + server_address + ":5558"
#    subscriber_address = "tcp://192.168.0." + server_address + ":5559"
# else:
# On the local computer: use localhost
leadaddress = "192.196.0.100"#"localhost"
w1 = "192.196.0.101"#"localhost"
w2 = "192.196.0.102"#"localhost"
pull_address = "tcp://"+leadaddress+":5557"
pull_address_worker_1 = "tcp://"+w1+":5561"
pull_address_worker_2 = "tcp://"+w2+":5565"
push_address = "tcp://"+leadaddress+":5558"
push_address_worker_1 = "tcp://"+w1+":5562"
push_address_worker_2 = "tcp://"+w2+":5566"
subscriber_address = "tcp://"+leadaddress+":5559"
subscriber_address_worker_1 = "tcp://"+w1+":5564"
subscriber_address_worker_2 = "tcp://"+w2+":5568"

context = zmq.Context()
# Socket to receive Store Chunk messages from the controller
receiver_direct_lead_node = context.socket(zmq.PULL)
receiver_direct_lead_node.connect(pull_address)
print("Listening on " + pull_address)

receiver_worker_1 = context.socket(zmq.PULL)
receiver_worker_1.connect(pull_address_worker_1)
print("Listening on " + pull_address_worker_1)

receiver_worker_2 = context.socket(zmq.PULL)
receiver_worker_2.connect(pull_address_worker_2)
print("Listening on " + pull_address_worker_2)

# Socket to send results to the Erasure Coding (Lead)
sender_lead_node = context.socket(zmq.PUSH)
sender_lead_node.connect(push_address)

# Socket to send results to the Erasure Coding (Random) Worker 1
sender_worker_1 = context.socket(zmq.PUSH)
sender_worker_1.connect(push_address_worker_1)

# Socket to send results to the Erasure Coding (Random) Worker 2
sender_worker_2 = context.socket(zmq.PUSH)
sender_worker_2.connect(push_address_worker_2)

# Socket to receive Get Chunk messages from the Lead node
subscriber_lead_node = context.socket(zmq.SUB)
subscriber_lead_node.connect(subscriber_address)

# Socket to receive Get Chunk messages from the Worker node 1
subscriber_worker_1 = context.socket(zmq.SUB)
subscriber_worker_1.connect(subscriber_address_worker_1)

# Socket to receive Get Chunk messages from the Worker node 2
subscriber_worker_2 = context.socket(zmq.SUB)
subscriber_worker_2.connect(subscriber_address_worker_2)

# Receive every message (empty subscription)
subscriber_lead_node.setsockopt(zmq.SUBSCRIBE, b'')
subscriber_worker_1.setsockopt(zmq.SUBSCRIBE, b'')
subscriber_worker_2.setsockopt(zmq.SUBSCRIBE, b'')

# Use a Poller to monitor three sockets at the same time
poller = zmq.Poller()
poller.register(receiver_direct_lead_node, zmq.POLLIN)
poller.register(receiver_worker_1, zmq.POLLIN)
poller.register(receiver_worker_2, zmq.POLLIN)
poller.register(subscriber_lead_node, zmq.POLLIN)
poller.register(subscriber_worker_1, zmq.POLLIN)
poller.register(subscriber_worker_2, zmq.POLLIN)

while True:
    try:
        # Poll all sockets
        socks = dict(poller.poll())
    except KeyboardInterrupt:
        break
    pass

    # At this point one or multiple sockets may have received a message

    # After Encoding Only Lead
    if receiver_direct_lead_node in socks:
        helper_functions.recv(receiver_direct_lead_node, sender_lead_node, messages_pb2, data_folder)

    # After Encoding Random Worker1
    if receiver_worker_1 in socks:
        helper_functions.recv(receiver_worker_1, sender_worker_1, messages_pb2, data_folder)

    # After Encoding Random Worker2
    if receiver_worker_2 in socks:
        helper_functions.recv(receiver_worker_2, sender_worker_2, messages_pb2, data_folder)

    # After Decoding Only Lead
    if subscriber_lead_node in socks:
        helper_functions.sub(subscriber_lead_node, sender_lead_node, messages_pb2, data_folder)

    # After Decoding Random Worker1
    if subscriber_worker_1 in socks:
        helper_functions.sub(subscriber_worker_1, sender_worker_1, messages_pb2, data_folder)

    # After Decoding Random Worker2
    if subscriber_worker_2 in socks:
        helper_functions.sub(subscriber_worker_2, sender_worker_2, messages_pb2, data_folder)

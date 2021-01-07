import time

import kodo
import math
import random
import copy  # for deepcopy
from utils import random_string
import messages_pb2

STORAGE_NODES_NUM = 4

RS_CAUCHY_COEFFICIENTS = [
    bytearray([253, 126, 255, 127]),
    bytearray([126, 253, 127, 255]),
    bytearray([255, 127, 253, 126]),
    bytearray([127, 255, 126, 253])
]


def encoding_file(file_data, max_erasures, send_task_socket, response_socket):
    start_enc_time = time.time()

    # Make sure we can realize max_erasures with 4 storage nodes
    assert (max_erasures >= 0)
    assert (max_erasures < STORAGE_NODES_NUM)
    print(f"erasures1")
    # How many coded fragments (=symbols) will be required to reconstruct the encoded data. 
    symbols = (STORAGE_NODES_NUM - max_erasures)

    # The size of one coded fragment (total size/number of symbols, rounded up)
    symbol_size = math.ceil(len(file_data) / symbols)

    print(f"erasures2")
    # Kodo RLNC encoder using 2^8 finite field
    encoder = kodo.RLNCEncoder(kodo.field.binary8, symbols, symbol_size)
    encoder.set_symbols_storage(file_data)
    fragment_names = []

    print(f"erasures3")
    task = []
    symbol = []
    # Generate one coded fragment for each Storage Node
    for i in range(STORAGE_NODES_NUM):
        # Select the next Reed Solomon coefficient vector
        print(f"erasures5")
        coefficients = RS_CAUCHY_COEFFICIENTS[i]

        # Generate a coded fragment with these coefficients
        # (trim the coefficients to the actual length we need)
        symbol.append(encoder.produce_symbol(coefficients[:symbols]))
        print(f"erasures6")

        # Generate a random name for it and save
        name = random_string(8)
        fragment_names.append(name)

        # Send a Protobuf STORE DATA request to the Storage Nodes
        t = messages_pb2.storedata_request()
        t.filename = name
        task.append(t)

        # task[i] = messages_pb2.storedata_request()
        # task[i].filename = name

    end_enc_time = time.time()
    print(f"erasures4")
    print(send_task_socket)
    for i in range(STORAGE_NODES_NUM):
        send_task_socket.send_multipart([task[i].SerializeToString(), bytearray(symbol[i])])

    print(f"erasures8")
    # Wait until we receive a response for every fragment
    for task_nbr in range(STORAGE_NODES_NUM):
        resp = response_socket.recv_string()
        print('Received: %s' % resp)

    print(f"erasures9")

    Timediff = end_enc_time - start_enc_time

    f = open("erasure_coding_enc_"+ str(max_erasures) +".txt", "a")
    f.write(str(round(Timediff, 4)) + ', ')
    f.close()

    return fragment_names


def decoding_file(coded_fragments, max_erasures, file_size, data_req_socket, response_socket):
    # We need 4-max_erasures fragments to reconstruct the file, select this many
    # by randomly removing 'max_erasures' elements from the given chunk names.
    frag_names = copy.deepcopy(coded_fragments)
    for i in range(max_erasures):
        frag_names.remove(random.choice(frag_names))

    # Request the coded fragments in parallel
    for name in frag_names:
        task = messages_pb2.getdata_request()
        task.filename = name
        data_req_socket.send(task.SerializeToString())

    # Receive both chunks and insert them to 
    symbols = []
    for _ in range(len(frag_names)):
        result = response_socket.recv_multipart()
        # In this case we don't care about the received name, just use the 
        # data from the second frame
        symbols.append({"chunkname": result[0].decode('utf-8'), "data": bytearray(result[1])})

    start_dec_time = time.time()
    print(f"start_dec_time: {start_dec_time}")
    # time.sleep(1)
    # Reconstruct the original data with a decoder
    symbols_num = len(symbols)
    symbol_size = len(symbols[0]['data'])
    decoder = kodo.RLNCDecoder(kodo.field.binary8, symbols_num, symbol_size)
    data_out = bytearray(decoder.block_size())
    decoder.set_symbols_storage(data_out)

    for symbol in symbols:
        # Figure out the which coefficient vector produced this fragment
        # by checking the fragment name index 
        coefficient_idx = coded_fragments.index(symbol['chunkname'])
        coefficients = RS_CAUCHY_COEFFICIENTS[coefficient_idx]
        # Use the same coefficients for decoding (trim the coefficients to 
        # symbols_num to avoid nasty bugs)
        decoder.consume_symbol(symbol['data'], coefficients[:symbols_num])

    end_dec_time = time.time()
    Timediff = end_dec_time - start_dec_time
    print(f"end_dec_time: {end_dec_time}")
    print(f"Timediff: {Timediff}")

    f = open("erasure_coding_dec_" + str(max_erasures) + ".txt", "a")
    f.write(str(round(Timediff, 12)) + ', ')
    f.close()

    # Make sure the decoder successfully reconstructed the file
    assert (decoder.is_complete())
    return data_out[:file_size]

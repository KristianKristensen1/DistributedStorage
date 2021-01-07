# Exercise 3
from flask import Flask, make_response, g, request, send_file
import sqlite3
import zmq  # For ZMQ
import time  # For waiting a second for ZMQ connections
import io  # For sending binary data in a HTTP response
import logging
import erasure_coding
import os.path
from asgiref.sync import async_to_sync, sync_to_async
# from utils import is_raspberry_pi

def get_db():
    if 'db' not in g:
        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        db_path = os.path.join(BASE_DIR, "files.db")
        g.db = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
        g.db.row_factory = sqlite3.Row
    return g.db


def close_db(e=None):
    db = g.pop('db', None)

    if db is not None:
        db.close()


# Initiate ZMQ sockets
context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://""*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

# Socket to send erasure coding tasks to Storage Nodes
send_task_socket_storage_worker_node = context.socket(zmq.PUSH)
send_task_socket_storage_worker_node.bind("tcp://*:5560")

# Socket to receive from worker nodes
receive_task_socket_worker_node = context.socket(zmq.PULL)
receive_task_socket_worker_node.bind("tcp://*:5563")

# Wait for all workers to start and connect.
time.sleep(1)
print("Listening to ZMQ messages on tcp://*:5558")

# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(close_db)


## Lead node (a)
@app.route('/files_lead', methods=['POST'])  # Add (Erasure) - Lead
def add_files_lead():
    start_time = time.time()
    # Flask separates files from the other form fields
    payload = request.form
    files = request.files

    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)

    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    # Reed Solomon code
    # Parse max_erasures (everything is a string in request.form,
    # we need to convert to int manually), set default value to 1
    max_erasures = int(payload.get('max_erasures', 2))
    print(f"Max erasures: {max_erasures}")
    # Store the file contents with Reed Solomon erasure coding
    fragment_names = erasure_coding.encoding_file(data, max_erasures, send_task_socket, response_socket)
    storage_details = {"coded_fragments": fragment_names, "max_erasures": max_erasures}

    # Insert the File record in the DB
    import json
    db = get_db()
    cursor = db.execute(
        "INSERT INTO 'file'(filename, size, content_type, storage_details) VALUES (?,?,?,?)",
        (filename, size, content_type, json.dumps(storage_details))
    )
    db.commit()

    end_time = time.time()
    Timediff  = end_time - start_time

    ff = open("erasure_coding_post_lead.txt", "a")
    ff.write(str(round(Timediff, 4)) + ', ')
    ff.close()

    return make_response({"id": cursor.lastrowid}, 201)


@app.route('/files_lead/<int:file_id>', methods=['GET'])  # Download (Erasure) - Lead
def download_file_lead(file_id):
    start_time = time.time()

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f['filename']))

    # Parse the storage details JSON string
    import json
    storage_details = json.loads(f['storage_details'])

    coded_fragments = storage_details['coded_fragments']
    max_erasures = storage_details['max_erasures']

    file_data = erasure_coding.decoding_file(coded_fragments, max_erasures,
                                             f['size'], data_req_socket,
                                             response_socket)

    end_time = time.time()
    Timediff = end_time - start_time

    ff = open("erasure_coding_get_lead.txt", "a")
    ff.write(str(round(Timediff, 4)) + ', ')
    ff.close()

    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])


## Random node (b)
@app.route('/files_random', methods=['POST'])  # Add (Erasure) - Random
def add_files_random():
    start_time = time.time()

    # Flask separates files from the other form fields
    payload = request.form
    files = request.files

    # Make sure there is a file in the request
    if not files or not files.get('file'):
        logging.error("No file was uploaded in the request!")
        return make_response("File missing!", 400)

    # Reference to the file under 'file' key
    file = files.get('file')
    # The sender encodes a the file name and type together with the file contents
    filename = file.filename
    content_type = file.mimetype
    # Load the file contents into a bytearray and measure its size
    data = bytearray(file.read())
    size = len(data)
    print("File received: %s, size: %d bytes, type: %s" % (filename, size, content_type))

    # Reed Solomon code
    # Parse max_erasures (everything is a string in request.form,
    # we need to convert to int manually), set default value to 1
    max_erasures = int(payload.get('max_erasures', 2))
    print(f"Max erasures: {max_erasures}")

    storage_details = async_random_node_encode(data, max_erasures, start_time)

    # Insert the File record in the DB
    import json
    db = get_db()
    cursor = db.execute("INSERT INTO 'file'(filename, size, content_type, storage_details) VALUES (?,?,?,?)",
                        (filename, size, content_type, json.dumps(storage_details)))
    db.commit()

    end_time = time.time()
    Timediff = end_time - start_time

    ff = open("erasure_coding_post_worker.txt", "a")
    ff.write(str(round(Timediff, 4)) + ', ')
    ff.close()

    return make_response({"id": cursor.lastrowid}, 201)


@app.route('/files_random/<int:file_id>', methods=['GET'])  # Download (Erasure) - Random
def download_file_random(file_id):
    start_time = time.time()

    db = get_db()
    cursor = db.execute("SELECT * FROM `file` WHERE `id`=?", [file_id])
    if not cursor:
        return make_response({"message": "Error connecting to the database"}, 500)

    f = cursor.fetchone()
    if not f:
        return make_response({"message": "File {} not found".format(file_id)}, 404)

    file = async_random_node_decode(f)

    end_time = time.time()
    Timediff = end_time - start_time

    ff = open("erasure_coding_get_worker.txt", "a")
    ff.write(str(round(Timediff, 4)) + ', ')
    ff.close()

    return send_file(io.BytesIO(file), mimetype=f['content_type'])

@async_to_sync
async def async_random_node_encode(data, max_erasures, start_time):
    send_task_socket_storage_worker_node.send_multipart(
        [bytes(str('encode'), 'utf-8'),
         data,
         bytes(str(max_erasures), 'utf-8')])

    end_time = time.time()
    Timediff = end_time - start_time
    ff = open("erasure_coding_post_worker_free_lead.txt", "a")
    ff.write(str(round(Timediff, 4)) + ', ')
    ff.close()

    message = receive_task_socket_worker_node.recv_multipart()
    print(f"Encode - Lead node - Data received {message}")
    fragment_names = []
    for i in range(4):
        fragment_names.append(str(message[i], 'utf-8'))
    print(fragment_names)

    storage_details = {"coded_fragments": fragment_names, "max_erasures": max_erasures}
    return storage_details


@async_to_sync
async def async_random_node_decode(f):
    # Convert to a Python dictionary
    f = dict(f)
    print("File requested: {}".format(f['filename']))

    # Parse the storage details JSON string
    import json
    storage_details = json.loads(f['storage_details'])

    fragment_names = storage_details['coded_fragments']
    max_erasures = storage_details['max_erasures']

    send_task_socket_storage_worker_node.send_multipart(
        [bytes(str('decode'), 'utf-8'),
         bytes(str(max_erasures), 'utf-8'),
         bytes(str(f['size']), 'utf-8'),
         bytes(fragment_names[0], 'utf-8'),
         bytes(fragment_names[1], 'utf-8'),
         bytes(fragment_names[2], 'utf-8'),
         bytes(fragment_names[3], 'utf-8')])

    message = receive_task_socket_worker_node.recv_multipart()
    file = message[0]
    return file

@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost"  # Listen for connections on the local computer
host_local_network = "0.0.0.0"  # Listen for connections on the local network
app.run(host=host_local_network, port=9000)  # if is_raspberry_pi() else host_local_computer, port=9000)

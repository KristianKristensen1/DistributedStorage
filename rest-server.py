"""
Aarhus University - Distributed Storage course - Lab 4

REST Server, starter template for Week 4
"""
from flask import Flask, make_response, g, request, send_file
import sqlite3
import base64
import random, string
import logging

import zmq # For ZMQ
import time # For waiting a second for ZMQ connections
import math # For cutting the file in half
import random # For selecting a random half when requesting chunks
import messages_pb2 # Generated Protobuf messages
import io # For sending binary data in a HTTP response
import utils
import replication

def random_string(length=8):
    """
    Returns a random alphanumeric string of the given length. 
    Only lowercase ascii letters and numbers are used.

    :param length: Length of the requested random string 
    :return: The random generated string
    """
    return ''.join([random.SystemRandom().choice(string.ascii_letters + string.digits) for n in range(length)])

def write_file(data, filename=None):
    """
    Write the given data to a local file with the given filename

    :param data: A bytes object that stores the file contents
    :param filename: The file name. If not given, a random string is generated
    :return: The file name of the newly written file, or None if there was an error
    """
    if not filename:
        # Generate random filename
        filename = random_string(length=8)
        # Add '.bin' extension
        filename += ".bin"
    
    try:
        # Open filename for writing binary content ('wb')
        # note: when a file is opened using the 'with' statment, 
        # it is closed automatically when the scope ends
        with open('./'+filename, 'wb') as f:
            f.write(data)
    except EnvironmentError as e:
        print("Error writing file: {}".format(e))
        return None
    
    return filename
#

# Initiate ZMQ sockets
context = zmq.Context()

# Socket to send tasks to Storage Nodes
send_task_socket = context.socket(zmq.PUSH)
send_task_socket.bind("tcp://*:5557")

# Socket to receive messages from Storage Nodes
response_socket = context.socket(zmq.PULL)
response_socket.bind("tcp://*:5558")

# Publisher socket for data request broadcasts
data_req_socket = context.socket(zmq.PUB)
data_req_socket.bind("tcp://*:5559")

# Wait for all workers to start and connect.
time.sleep(1)
print ("Listening to ZMQ messages on tcp://*:5558")



# Instantiate the Flask app (must be before the endpoint functions)
app = Flask(__name__)
# Close the DB connection after serving the request
app.teardown_appcontext(utils.close_db)

@app.route('/')
def hello():
    return make_response({'message': 'Hello World!'})

@app.route('/files',  methods=['GET'])
def list_files():

    return 0
 #Functionality to get files
#

@app.route('/files/<int:file_id>',  methods=['GET'])
def download_file(file_id):
    db = utils.get_db()
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
    print(f['storage_mode'])

    if f['storage_mode'] == 'replication1':
        filename = storage_details['replication_filename']

        file_data = replication.get_file(
            filename, 
            data_req_socket, 
            response_socket
        )
        

    return send_file(io.BytesIO(file_data), mimetype=f['content_type'])
#

@app.route('/files', methods=['POST'])
def add_files():
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
    
    # Read the requested storage mode from the form (default value: 'replication1')
    storage_mode = payload.get('storage', 'replication1')
    print("Storage mode: %s" % storage_mode)

    if storage_mode == 'replication1':
        # Set number of replicas to be saved (default value: 1)
        replication_number = payload.get('replicas', 1)
        replication_name = replication.store_file(data, replication_number, send_task_socket, response_socket)
        storage_details = {
            "replication_filename": replication_name
        }

    elif storage_mode == 'erasure_coding_rs':
        print("Erasure_Coding_rs")

    elif storage_mode == 'erasure_coding_rlnc':
        print("Erasure_coding_rlnc")

    elif storage_mode == 's3' :
        print("s3")

    else:
        logging.error("Unexpected storage mode: %s" % storage_mode)
        return make_response("Wrong storage mode", 400)

    # Insert the File record in the DB
    import json
    db = utils.get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `storage_details`) VALUES (?,?,?,?,?)",
        (filename, size, content_type, storage_mode, json.dumps(storage_details))
    )
    db.commit()

    return make_response({"id": cursor.lastrowid }, 201)


import logging
@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=9000)
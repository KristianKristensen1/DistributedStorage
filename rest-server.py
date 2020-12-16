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
import json
import urllib.request
import threading
import time


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

# Load the datanode IP addresses from datanodes.txt 
DATANODE_ADDRESSES = utils.read_file_by_line('datanodes.txt')
print("Using %d Datanodes:\n%s\n" % (len(DATANODE_ADDRESSES), '\n'.join(DATANODE_ADDRESSES)))

@app.after_request
def add_cors_headers(response):
    """
    Add Cross-Origin Resource Sharing headers to every response.
    Without these the browser does not allow the HDFS webapp to process the response.
    """
    response.headers['Access-Control-Allow-Origin'] = '*'
    response.headers['Access-Control-Allow-Headers'] = 'Authorization,Content-Type'
    response.headers['access-control-allow-methods'] = 'DELETE, GET, OPTIONS, POST, PUT'
    return response
#

@app.route('/files',  methods=['GET'])
def list_files():
    db = utils.get_db()
    cursor = db.execute( "SELECT * FROM `file`" )
    if not cursor:
        return make_response({ "message" : "Error connecting to the database" }, 500 )
    files = cursor.fetchall()

    # Convert files from sqlite3.Row object (which is not JSON-encodable) to
    # a standard Python dictionary simply by casting
    files = [ dict ( file ) for file in files]
    return make_response({ "files" : files})
#

@app.route('/done',  methods=['POST'])
def done():
    print("Done is called!")
    rep2End = time.time()
    file_id = request.get_json()
    db = utils.get_db()
    cursor = db.execute("SELECT `recieved_time` FROM `file` WHERE `id`=?", [file_id])
    f = cursor.fetchone()
    f = dict(f)
    print(f)
    rep2Start = f['recieved_time']
    Timediff = rep2End - rep2Start
    f = open("replication2_redundancy_done.txt", "a")
    f.write(str(round(Timediff, 4)) + ', ')
    f.close()
    return make_response("Shit went well! ")

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
 
    if f['storage_mode'] == 'replication1' or 'replication2':
        file_locations = f['replica_locations']
        locations = file_locations.split()
        for i in range(len(locations)):
            try:
                file_data = replication.get_file(file_id, locations[i])
                break
            except:
                continue

    return send_file(file_data, mimetype=f['content_type'])
#

@app.route('/files', methods=['POST'])
def add_files():    
    postStart = time.time()
    payload = request.get_json()
    file_data = base64.b64decode(payload.get( 'file_data' ))
    filename = payload.get( 'filename' )
    filetype = payload.get( 'type' )
    storage_mode = payload.get('storage')
    size = len(file_data)

    if storage_mode == 'replication1':
        # Set number of replicas to be saved (default value: 1)
        replication_number = payload.get('replicas', 1)

        # Sample k random storage location adresses 
        replica_locations = random.sample(DATANODE_ADDRESSES, k = int(replication_number))

        file_id = replication.store_db(filename, size, filetype, storage_mode, replica_locations, 0)

        # Save replica in each location
        for _ in range(len(replica_locations)):
            result = replication.store_file(replica_locations, payload, file_id)
            replica_locations = replica_locations[ 1 :]
        postEnd = time.time()
        Timediff = postEnd - postStart
        f = open("replication1_redundancy_done.txt", "a")
        f.write(str(round(Timediff, 4)) + ', ')
        f.close()
        return result

    elif storage_mode == 'replication2':
        # Set number of replicas to be saved (default value: 1)
        replication_number = payload.get('replicas', 1)

        # Sample k random storage location adresses 
        replica_locations = random.sample(DATANODE_ADDRESSES, k = int(replication_number))

        file_id = replication.store_db(filename, size, filetype, storage_mode, replica_locations, postStart)

        # Send file to first node and return response to client
        if len (replica_locations):
            t1 = threading.Thread(target=replication.store_file, args=(replica_locations, payload, file_id,))
            t1.start()
            lead_done = time.time()
            Timediff = lead_done - postStart
            f = open("replication2_lead_done.txt", "a")
            f.write(str(round(Timediff, 4)) + ', ')
            f.close()
            return make_response({"File: ": file_id,"File saved in locations: ": replica_locations})


import logging
@app.errorhandler(500)
def server_error(e):
    logging.exception("Internal error: %s", e)
    return make_response({"error": str(e)}, 500)


# Start the Flask app (must be after the endpoint functions) 
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
app.run(host=host_local_computer, port=9000)
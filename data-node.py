from flask import Flask, make_response, request, send_file
import sys, os, io, logging
import random, base64
import urllib.request
import json
import threading
import replication
import time

import utils

# Read the folder name where chunks should be stored from the first program argument
# (or use the current folder if none was given)
data_folder = sys.argv[1] if len(sys.argv) > 1 else "./"
if data_folder != "./":
    # Try to create the folder  
    try:
        os.mkdir('./'+data_folder)
    except FileExistsError as _:
        # OK, the folder exists 
        pass
print("Data folder: %s" % data_folder)

app = Flask(__name__)

leadNode = "http://localhost:9000"

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

"""
Datanode - Store File data
Endpoint: POST /write
Request body: file contents (base64 encoded), file ID and list of remaining datanode addresses
Response: Empty
"""
@app.route ( '/write' , methods =[ 'POST' ])
def write_file ():
    payload = request.get_json()
    file_data = base64.b64decode(payload.get( 'file_data' ))
    file_id = payload.get( 'file_id' )
    replica_locations = payload.get( 'replica_locations' )

    

    filename = "{}/file-{}.bin".format(data_folder, file_id)
    if not utils.write_file(file_data, filename):
        logging.error( "Error saving %d bytes data" % len (file_data))
        return make_response({ 'message' , 'Error storing file' }, 500 )

    if (len(replica_locations) > 0 and payload[ 'storage' ] == "replication2"):
        t1 = threading.Thread(target=replication.store_file, args=(replica_locations, payload, file_id,))
        t1.start()
        return make_response({"File: ": file_id,"File saved in locations: ": replica_locations})
    
    elif(payload[ 'storage' ] == "replication2"):
        req = urllib.request.Request(leadNode+ '/done')
        req.add_header( 'Content-Type' , 'application/json; charset=utf-8' )
        payload_bytes = json.dumps(file_id).encode( 'utf-8' ) # needs to be bytes
        req.add_header( 'Content-Length' , len (payload_bytes))
        urllib.request.urlopen(req, payload_bytes)
    return make_response({"File saved successfully with id: ": file_id})

"""
Datanode - Download file data
Endpoint: GET /read/[fileID]
Request body: Empty
Response: Binary file contents
"""
@app.route ( '/read/<int:file_id>' , methods =[ 'GET' ])
def read_file ( file_id ):
    filename = "{}/file-{}.bin" .format(data_folder, file_id)
    file_contents = None
    with open (filename, "rb" ) as f:
        file_contents = f.read()
    return send_file(io.BytesIO(file_contents), mimetype = 'application/octet-stream' )


# Start the Flask app (must be after the endpoint functions)
host_local_computer = "localhost" # Listen for connections on the local computer
host_local_network = "0.0.0.0" # Listen for connections on the local network
port = 9000 if utils.is_raspberry_pi() else random.randint(9000, 9500)
print("Starting Datanode on port %d\n\n" % port)
app.run(host=host_local_network if utils.is_raspberry_pi() else host_local_computer, port=port)

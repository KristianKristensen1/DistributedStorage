import messages_pb2
import math
import random
import urllib.request
import json
import utils


from utils import random_string

def store_file(replica_locations, payload, file_id):
    target_dn = replica_locations[ 0 ]
    payload[ 'replica_locations' ] = replica_locations[ 1 :]
    replica_locations = replica_locations[ 1 :]
    payload[ 'file_id' ] = file_id
    # Construct HTTP POST request to the next Datanode (re-send the payload we got)
    req = urllib.request.Request(target_dn+ '/write' )
    req.add_header( 'Content-Type' , 'application/json; charset=utf-8' )
    payload_bytes = json.dumps(payload).encode( 'utf-8' ) # needs to be bytes
    req.add_header( 'Content-Length' , len (payload_bytes))
    # Send the request and wait for the response
    urllib.request.urlopen(req, payload_bytes)
    return {"File saved successfully with id: ": file_id}


def get_file(file_id, file_location):
    req = urllib.request.Request(file_location + "/read/" + str(file_id))
    response = urllib.request.urlopen(req)
    return response

def store_db(filename, size, filetype, storage_mode, replica_locations, recieved_time):
    # Insert the File record in the DB
    db = utils.get_db()
    cursor = db.execute(
        "INSERT INTO `file`(`filename`, `size`, `content_type`, `storage_mode`, `replica_locations`, recieved_time) VALUES (?,?,?,?,?,?)" ,
        (filename, size, filetype, storage_mode, ' ' .join(replica_locations), recieved_time)
    )
    db.commit()
    file_id = cursor.lastrowid
    return file_id
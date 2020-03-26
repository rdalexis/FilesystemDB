import os

def globalsInit():
    # mysql connectors
    global cnx, cursor, qryrecords
    cnx = None
    cursor = None
    qryrecords = None
    
    global current_fid, current_fldr_contents
    current_fid = 0
    current_fldr_contents = []

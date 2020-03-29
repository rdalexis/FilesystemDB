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

    # fids permanent
    fidroot = 1
    fiduser = 2 # parent should be home dir
    fidhome = 0

    # terminal path string
    global terminalpath
    terminalpath = ""
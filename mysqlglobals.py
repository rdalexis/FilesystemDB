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
    global fidroot, fidhome, fiduser
    fidroot = 0
    fidhome = 2    
    fiduser = 3 # parent should be home dir

    # terminal path string
    global terminalpath
    terminalpath = ""

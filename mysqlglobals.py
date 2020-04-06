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
    fidhome = 0    
    fiduser = 0 # parent should be home dir

    # terminal path string
    global terminalpath
    terminalpath = ""

    # user and group dictionaries
    global users, groups
    users = {}
    groups = {}

    global PATH, pathfids
    PATH = "/home/mk/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin"
    pathfids = []

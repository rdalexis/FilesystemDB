from __future__ import print_function

import os
import mysql.connector
from mysql.connector import errorcode

# Default size is 64MB. So, setting 50MB
DEFAULT_MAX_ALLOWABLE_PACKET = 524288000
DB_NAME = 'filesystem'
TABLES_IN_DB = ['tree', 'fattrb', 'fdata']

DROP_TREE = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[0]
DROP_FATTRB = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[1]
DROP_FDATA = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[2]

TABLES = {}
TABLES[TABLES_IN_DB[0]] = (
    "CREATE TABLE `tree` ("
    "`fid` int(10) unsigned NOT NULL auto_increment,"
    "`parentid` int(10) unsigned default NULL,"
    "`name` varchar(255) character set utf8 collate utf8_bin NOT NULL,"
    "UNIQUE KEY `name` (`name`,`parentid`),"
    "KEY `fid` (`fid`),"
    "KEY `parentid` (`parentid`)"
    ") DEFAULT CHARSET=utf8"
    )

TABLES[TABLES_IN_DB[1]] = (
   "CREATE TABLE `fattrb` ("
   "`fid` bigint(20) NOT NULL,"
   "`mode` int(11) NOT NULL default '0',"
   "`uid` int(10) unsigned NOT NULL default '0',"
   "`gid` int(10) unsigned NOT NULL default '0',"
   "`nlink` int(10) unsigned NOT NULL default '0',"
   "`mtime` int(10) unsigned NOT NULL default '0',"
   "`size` bigint(20) NOT NULL default '0',"
   "PRIMARY KEY  (`fid`)"
   ") DEFAULT CHARSET=binary"
   )

"""`seq` int unsigned NOT NULL,"""
TABLES[TABLES_IN_DB[2]] = (
   "CREATE TABLE `fdata` ("
   "`fid` bigint(20) NOT NULL,"
   "`data` longblob,"
   "PRIMARY KEY  (`fid`)"
   ")  DEFAULT CHARSET=binary"
   )

add_tree_entry = ("INSERT INTO tree (fid, parentid, name) VALUES (%s, %s, %s)")
add_fattrb_entry = ("INSERT INTO fattrb (fid, mode, uid, gid, nlink, mtime, size) VALUES (%s, %s, %s, %s, %s, %s, %s)")
add_fdata_entry = ("INSERT INTO fdata (fid, data) VALUES (%s, %s)")
#add_fdata_entry = ("INSERT INTO fdata (fid, seq, data) VALUES (%s, %s, %s)")

fileid = 0      

def open_database(cursor, dbname):
    try:
        cursor.execute("USE {}".format(dbname))
    except mysql.connector.Error as err:
        print("Failed opening database: {}".format(err))
        return 1

def create_database(cnx, cursor, dbname, fspath):
    try:
        cursor.execute(
            "DROP DATABASE IF EXISTS {}".format(dbname))
    except mysql.connector.Error as err:
        print("Failed dropping database :", dbname)
        return 1
    else:
        print("Creating database : {}".format(dbname))

    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(dbname))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        return 1

    # Open database
    if (open_database(cursor, dbname) == 1):
        return 1

    # Create tables
    for table_name in TABLES:
        table_description = TABLES[table_name]
        try:
            cursor.execute(table_description)
        except mysql.connector.Error as err:
            print(err.msg)
        else:
            print("Creating table : {}".format(table_name))   

    # populate data 
    scan_directories(cursor, fspath, None)
    # There will be no update after this point, hence commit
    cnx.commit()

"""
def read_in_chunks(file_object, chunk_size=DEFAULT_MAX_ALLOWABLE_PACKET):
    while True:
        try:
           data = file_object.read(chunk_size)
           if not data:
              break
           yield data
        except:
           return None
def file_data(fid, fentry):
       seq = 0
       file_to_read = open(fentry, 'rb')
       for piece in read_in_chunks(file_to_read):
           fdata = []
           fdata.append(fid)
           fdata.append(seq)
           fdata.append(piece)
           cursor.execute(add_fdata_entry, fdata)
           seq = seq + 1
       file_to_read.close()
"""
def file_data(cursor, fid, fentry):
    try:
       file_to_read = open(fentry, 'rb')
       file_content = file_to_read.read()
       file_to_read.close()
    except:
       print('Cannot read '+str(fentry.name))
       file_content = None
    fdata = []
    fdata.append(fid)
    fdata.append(file_content)
    cursor.execute(add_fdata_entry, fdata)

def f_attributes(cursor, fid, attr):
    if not attr is None:
       fattrb = []
       fattrb.append(fid)
       fattrb.append(attr.st_mode)
       fattrb.append(attr.st_uid)
       fattrb.append(attr.st_gid)
       fattrb.append(attr.st_nlink)
       fattrb.append(attr.st_mtime)
       fattrb.append(attr.st_size)
       cursor.execute(add_fattrb_entry, fattrb)

def scan_directories(cursor, path, parentid):
    global fileid
    with os.scandir(path) as dir_entries:
        for entry in dir_entries:
            print(entry.name)
            tree_entry = []
            fileid = fileid + 1
            info = None
            
            try:
                info = entry.stat()
            except:
                pass
            if info is not None:
                f_attributes(cursor, fileid, info)
            
                if os.path.isdir(entry):  
                   tree_entry.append(fileid)
                   tree_entry.append(parentid)
                   tree_entry.append(entry.name) 
                   cursor.execute(add_tree_entry, tree_entry)
                   parentid1 = fileid
                   fileid = scan_directories(cursor, entry, parentid1)  
                else:  
                   tree_entry.append(fileid)
                   tree_entry.append(parentid)
                   tree_entry.append(entry.name)
                   cursor.execute(add_tree_entry, tree_entry) 
                   file_data(cursor, fileid, entry)
            else:
                fileid = fileid - 1
    return fileid
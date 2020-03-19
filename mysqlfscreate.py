from __future__ import print_function

import os
import mysql.connector
from mysql.connector import errorcode

DB_NAME = 'filesystem'

TABLES = {}
TABLES['tree'] = (
    "CREATE TABLE `tree` ("
    "`fid` int(10) unsigned NOT NULL auto_increment,"
    "`parentid` int(10) unsigned default NULL,"
    "`name` varchar(255) NOT NULL,"
    "UNIQUE KEY `name` (`name`,`parentid`),"
    "KEY `fid` (`fid`),"
    "KEY `parentid` (`parentid`)"
    ") DEFAULT CHARSET=utf8"
    )

cnx = mysql.connector.connect(user='dba', password='Password123$',
                              host='127.0.0.1')
cursor = cnx.cursor();

def CreateFSDatabase(cursor):
    try:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(DB_NAME))
    except mysql.connector.Error as err:
        print("Failed creating database: {}".format(err))
        exit(1)

try:
    cursor.execute("USE {}".format(DB_NAME))
except mysql.connector.Error as err:
    print("Database {} does not exists.".format(DB_NAME))
    if err.errno == errorcode.ER_BAD_DB_ERROR:
        create_database(cursor)
        print("Database {} created successfully.".format(DB_NAME))
        cnx.database = DB_NAME
    else:
        print(err)
        exit(1)

for table_name in TABLES:
    table_description = TABLES[table_name]
    try:
        print("Creating table {}: ".format(table_name), end='')
        cursor.execute(table_description)
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
            print("already exists.")
        else:
            print(err.msg)
    else:
        print("OK")

add_tree_entry = ("INSERT INTO tree "
               "(fid, parentid, name) "
               "VALUES (%s, %s, %s)")

#tree_entry = list()
fileid = 0
#parentid = None

def scan_directories(path, parentid):
    global fileid
    tree_entry = []
    with os.scandir(path) as dir_entries:
        for entry in dir_entries:
            print(entry.name)
            fileid = fileid + 1
            if os.path.isdir(entry):  
               tree_entry.append(fileid)
               tree_entry.append(parentid)
               tree_entry.append(entry.name) 
               cursor.execute(add_tree_entry, tree_entry)
               parentid1 = fileid
               #fileid = fileid + 1
               tree_entry = []
               fileid = scan_directories(entry, parentid1)
               print("\nIt is a directory")  
            elif os.path.isfile(entry):  
               tree_entry.append(fileid)
               tree_entry.append(parentid)
               tree_entry.append(entry.name)
               cursor.execute(add_tree_entry, tree_entry)
               #fileid = fileid + 1
               tree_entry = []
               print("\nIt is a normal file")
            else:  
                print("It is a special file (socket, FIFO, device file)" )
            #print(fid)
            info = entry.stat()
            #print(info)
    return fileid

scan_directories('.', None);
cnx.commit()
cursor.close()
cnx.close()


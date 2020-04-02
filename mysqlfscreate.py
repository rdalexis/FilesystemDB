from __future__ import print_function

import os
import mysql.connector
from mysql.connector import errorcode

# Default size is 64MB. So, setting 50MB
DEFAULT_MAX_ALLOWABLE_PACKET = 524288000
DB_NAME = 'filesystem'
TABLES_IN_DB = ['tree', 'fattrb', 'fdata', 'link']

DROP_TREE = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[0]
DROP_FATTRB = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[1]
DROP_FDATA = "DROP TABLE IF EXISTS "+ TABLES_IN_DB[2]

TABLES = {}
TABLES[TABLES_IN_DB[0]] = (
    "CREATE TABLE `tree` ("
    "`fid` bigint(20) unsigned NOT NULL auto_increment,"
    "`parentid` bigint(20) unsigned default NULL,"
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

TABLES[TABLES_IN_DB[2]] = (
   "CREATE TABLE `fdata` ("
   "`fid` bigint(20) NOT NULL,"
   "`data` longblob,"
   "PRIMARY KEY  (`fid`)"
   ")  DEFAULT CHARSET=binary"
   )

TABLES[TABLES_IN_DB[3]] = (
   "CREATE TABLE `link` ("
   "`sfid` bigint(20) NOT NULL,"
   "`tfid` bigint(20) NOT NULL,"
   "PRIMARY KEY  (`sfid`)"
   ")  DEFAULT CHARSET=binary"
   )

GREP_SP = ( 
"DELIMITER $$"
"CREATE PROCEDURE grep("
"    IN file_id BIGINT,"
"    IN search_string VARCHAR(255)"
")"
"BEGIN"
"    DECLARE NumOfLines INT DEFAULT 0;"
"    DECLARE IterationCount INT DEFAULT 0;"
"    DECLARE ExtractedData LONGBLOB;"
"    DECLARE CurrentLine VARCHAR(255) DEFAULT '';"
"    DECLARE StringPosition INT DEFAULT 0;"
"    DECLARE TempData LONGBLOB;"
"    SELECT ROUND((length(data)-length(replace(data, '\n', "")))/length('\n')) INTO NumOfLines"
"    FROM fdata"
"    WHERE fid = file_id;"
"    SELECT data INTO TempData FROM fdata WHERE fid = file_id;"
"    WHILE IterationCount <= NumOfLines DO"
"        SELECT SUBSTRING_INDEX(TempData, '\n', IterationCount) INTO ExtractedData;"
"        SELECT SUBSTRING_INDEX(ExtractedData, '\n', -1) INTO CurrentLine;"
"        SELECT POSITION(search_string IN CurrentLine) INTO StringPosition;"
"        IF StringPosition > 0 THEN"
"           SELECT IterationCount AS LineNumber, CurrentLine;"
"        END IF;"
"        SET IterationCount = IterationCount + 1;"
"        SET CurrentLine = '';"
"        SET StringPosition = 0;"
"    END WHILE;"
"END$$"
"DELIMITER ;"
)

add_tree_entry = ("INSERT INTO tree (fid, parentid, name) VALUES (%s, %s, %s)")
add_fattrb_entry = ("INSERT INTO fattrb (fid, mode, uid, gid, nlink, mtime, size) VALUES (%s, %s, %s, %s, %s, %s, %s)")
add_fdata_entry = ("INSERT INTO fdata (fid, data) VALUES (%s, %s)")

fileid = 0

def open_database(cursor, dbname):
    try:
        cursor.execute("USE {}".format(dbname))
        #for line in open("./grep_stored_procedure.sql"):
            #cursor.execute(line)
        #cursor.execute(GREP_SP)
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

    # adding root, just for testing, TODO make it proper
    try:    
        cursor.execute(
            "INSERT tree(fid, parentid, name) VALUES(0,0,'/')")
        cursor.execute(
            "UPDATE tree SET fid = 0 WHERE name = '/'")
        cursor.execute(
            "INSERT fattrb(fid, mode, uid, gid, nlink, mtime, size)"
                " VALUES(0, 16877, 0, 0, 1, 0, 0)")  
        #cursor.execute(GREP_SP)          
    except mysql.connector.Error as err:
        print("Failed inserting value for root : {}".format(err))
        return 1            

    # populate data 
    scan_directories(cursor, fspath, 0)

    # TODO creating tree entry for ~, etc.,

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
       file_handle = open(fentry, 'rb')
       file_content = file_handle.read()
       file_handle.close()
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
                tree_entry.append(fileid)
                tree_entry.append(parentid)
                tree_entry.append(entry.name) 
                cursor.execute(add_tree_entry, tree_entry)
                if os.path.isdir(entry):  
                   parentid1 = fileid
                   fileid = scan_directories(cursor, entry, parentid1)  
                elif os.path.isfile(entry):  
                   file_data(cursor, fileid, entry)
                elif os.path.islink(entry):
                   print(str(entry.name)+' link')
                else:
                   dummy = None    
            else:
                fileid = fileid - 1
    return fileid

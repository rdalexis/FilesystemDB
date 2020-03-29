import os
import mysql.connector
from mysql.connector import errorcode

import mysqlglobals as gl

def query_execute(query):
    try:
        gl.cursor.execute(query)
        return 0
    except mysql.connector.Error as err:
        print("Failed executing query : ", query)
        print("Error : {}".format(err))
        return 1

def query_fetchresult_all():
    return list(gl.cursor.fetchall()) if gl.cursor.rowcount else list()

def query_fetchresult_one():
    return list(gl.cursor.fetchone()) if gl.cursor.rowcount else list()        

def call_procedure_selectresults(sp_name, sp_args):
    try:
        if (sp_args != ""):
            gl.cursor.callproc(sp_name, sp_args)
        else:
            gl.cursor.callproc(sp_name)
        return 0
    except mysql.connector.Error as err:
        print("Failed executing stored procedure : ", sp_name)
        print("Error : {}".format(err))
        return 1

def sp_fetch_selectresults_all():
    for result in gl.cursor.stored_results():
        return result.fetchall() 

def call_procedure_argresults(sp_name, sp_args):
    try:
        if (sp_args != ""):
            return gl.cursor.callproc(sp_name, sp_args)
        else:
            return gl.cursor.callproc(sp_name)
    except mysql.connector.Error as err:
        print("Failed executing stored procedure : ", sp_name)
        print("Error : {}".format(err))
        return 1

def get_childfid(parentfid, childdirname, isdirectory):

def get_parentfid(childfid):
    qry = "SELECT parentid FROM tree WHERE fid ="+str(childfid)
    if query_execute(qry) == 0:
        return query_fetchresult_one()[0]
    else:
        return -1

def get_fid_from_dirpath(currfid, dirpath):
    dirtraversed = []
    pathsplit = dirpath.split('/')

    for i in range(len(dirlist)):
        # Check for / at begin and end
        if pathsplit[i] == "":
            if i == 0:
                currfid = gl.fidroot
            else
                currfid = -1
                break
        elif pathsplit[i] == "~":
            if i == 0:
                currfid = gl.fidroot
                if gl.fidhome != 0:
                    dirtraversed.append(currfid)
                    dirtraversed.append(gl.fidhome)
                    currfid = gl.fiduser
            else:
                currfid = -1
                break
        elif pathsplit[i] == "..":
            if len(dirtraversed) == 0:
                currfid = get_parentfid(currfid)
            else:
                currfid = dirtraversed.pop()
        elif pathsplit[i] != "" :
            dirtraversed.append(currfid)
            currfid = get_childfid(currfid, pathsplit[i], True)
    
        if currfid == -1
            break

    return currfid

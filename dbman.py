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
    stored_proc_results = []
    for result in gl.cursor.stored_results():
        stored_proc_results.extend(result.fetchall()) 
    return stored_proc_results

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

def get_childfid(parentfid, childdirname, isdirectorycheck, isreturnmode):
    qry = "SELECT T.fid, F.mode, L.tfid FROM (SELECT fid FROM tree WHERE parentid = "\
        +str(parentfid)+" AND name LIKE '"+childdirname+"') T INNER JOIN fattrb F "\
            "ON T.fid = F.fid LEFT JOIN link L ON T.fid = L.sfid"
    if query_execute(qry) == 0:
        result = query_fetchresult_one()
        # print(qry)
        # print("query output : ", result)
        if (len(result) != 0):
            if (isdirectorycheck == True) and ((16384 & result[1]) != 16384):
                if isreturnmode == True: return -1, 0 
                else: return -1
            elif result[2] != None:
                if isreturnmode == True: return result[2], result[1] 
                else: return result[2]
            else:
                if isreturnmode == True: return result[0], result[1] 
                else: return result[0]
        else:
            if isreturnmode == True: return -1, 0 
            else: return -1

def get_parentfid(childfid):
    qry = "SELECT parentid FROM tree WHERE fid = "+str(childfid)
    if query_execute(qry) == 0:
        result = query_fetchresult_one()
        # print(qry)
        # print("query output : ", result)        
        if (len(result) != 0):
            return result[0]
        else:
            return -1
    else:
        return -1

def get_fid_from_dirpath(currfid, dirpath, isdirectorycheck = False, isreturnmode = False):
    dirtraversed = []
    pathsplit = dirpath.split('/')
    splitsize = len(pathsplit)
    mode = 0

    if splitsize > 1:
        if (isdirectorycheck == True and pathsplit[splitsize - 1] == ""):
            pathsplit.pop()
            splitsize = splitsize - 1
        elif (isdirectorycheck == False and pathsplit[splitsize - 1] == ""):
            return -1

    for i in range(splitsize):
        # Check for / at begin and end
        if pathsplit[i] == "":
            if i == 0:
                currfid = gl.fidroot
            else:
                currfid = -1
        elif pathsplit[i] == "~":
            if i == 0:
                currfid = gl.fidroot
                if gl.fidhome != 0:
                    dirtraversed.append(currfid)
                    dirtraversed.append(gl.fidhome)
                    currfid = gl.fiduser
            else:
                currfid = -1
        elif pathsplit[i] == "..":
            if len(dirtraversed) == 0:
                currfid = get_parentfid(currfid)
            else:
                currfid = dirtraversed.pop()
        elif pathsplit[i] == ".":
            continue
        else:
            dirtraversed.append(currfid)
            if(i == splitsize - 1):
                currfid, mode = get_childfid(currfid, pathsplit[i], isdirectorycheck, True)
            else:
                currfid = get_childfid(currfid, pathsplit[i], True, False)
    
        if currfid == -1:
            break

    if isreturnmode:
        return currfid, mode
    else:
        return currfid

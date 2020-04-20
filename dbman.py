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

def get_childfid(parentfid, childdirname, isdirectorycheck, isreturnfiletype):
    # qry = "SELECT T.fid, F.mode, L.tfid, (SELECT mode from fattrb WHERE fid = L.tfid) "\
    #     "FROM (SELECT fid FROM tree WHERE parentid = "\
    #     +str(parentfid)+" AND name LIKE '"+childdirname+"') T INNER JOIN fattrb F "\
    #         "ON T.fid = F.fid LEFT JOIN link L ON T.fid = L.sfid"
    # qry = "SELECT F.fid, F.filetype, L.linkfid, (SELECT filetype from fattrb WHERE fid = L.linkfid) "\
    #     "FROM (SELECT fid, filetype FROM fattrb WHERE parentid = "\
    #     +str(parentfid)+" AND name LIKE '"+childdirname+"') F"\
    #         " LEFT JOIN link L ON F.fid = L.fid" 
    
    # qry = "SELECT F.fid, F.filetype, L.linkfid, (SELECT filetype from fattrb WHERE fid = L.linkfid) "\
    #     "FROM fattrb F LEFT JOIN link L ON F.fid = L.fid WHERE F.parentid = "\
    #     +str(parentfid)+" AND F.name LIKE '"+childdirname+"'"

    qry = "SELECT F.fid, F.filetype, L.linkfid, "\
        "(SELECT B.filetype from tree A INNER JOIN fattrb B ON A.nodeid = B.nodeid WHERE A.fid = L.linkfid) "\
        "FROM (SELECT T.fid, C.filetype FROM tree T INNER JOIN fattrb C ON T.nodeid = C.nodeid "\
        "WHERE T.parentid = "+str(parentfid)+" AND C.name LIKE '"+childdirname+"') F "\
        "LEFT JOIN link L ON F.fid = L.fid"       

    if query_execute(qry) == 0:
        result = query_fetchresult_one()
        #print(qry)
        #print("query output : ", result)
        if (len(result) != 0):
            # get filetype
            if result[2] == None: filetype = result[1]
            else: filetype = result[3]
            
            if (isdirectorycheck == True) and ((16384 & int(filetype)) != 16384):
                if isreturnfiletype == True: return -1, 0 
                else: return -1
            elif result[2] != None:
                if isreturnfiletype == True: return result[2], filetype 
                else: return result[2]
            else:
                if isreturnfiletype == True: return result[0], filetype 
                else: return result[0]
        else:
            if isreturnfiletype == True: return -1, 0 
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

def get_fid_from_dirpath(currfid, dirpath, isdirectorycheck = False, isreturnfiletype = False):
    #print('dirpath is '+str(dirpath))
    dirtraversed = []
    pathsplit = dirpath.split('/')
    splitsize = len(pathsplit)
    filetype = 16384
    slashatend = False

    #print(dirpath, pathsplit, splitsize)

    if splitsize > 1:
        if splitsize == 2 and pathsplit[0] == "" and pathsplit[1] == "":
            # handling for just path : /
            if isreturnfiletype:
                return gl.fidroot, filetype
            else:
                return gl.fidroot

        # if (isdirectorycheck == True and pathsplit[splitsize - 1] == ""):
        #     pathsplit.pop()
        #     splitsize = splitsize - 1
        # elif (isdirectorycheck == False and pathsplit[splitsize - 1] == ""):
        #     if isreturnfiletype:
        #         return -1, 0
        #     else:
        #         return -1

        # handling for / at the end
        if (pathsplit[splitsize - 1] == ""):
            pathsplit.pop()
            splitsize = splitsize - 1
            slashatend = True        

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
                currfid, filetype = get_childfid(currfid, pathsplit[i], isdirectorycheck, True)
            else:
                currfid = get_childfid(currfid, pathsplit[i], True, False)
    
        if currfid == -1:
            filetype = 0
            break

    # only a directory can have slash at the end
    if slashatend == True:
        if (16384 & filetype) != 16384:
            currfid = -1

    if isreturnfiletype:
        return currfid, filetype
    else:
        return currfid

def get_folder_elements_with_attrib(fidfolder):
    # qry = "SELECT T.fid, T.name, mode, nlink, uid, gid, size, mtime, tfid"\
    #     " FROM (SELECT fid, name FROM tree WHERE parentid = "+str(fidfolder)+") T"\
    #     " INNER JOIN fattrb F ON T.fid = F.fid LEFT JOIN link L ON T.fid = L.sfid ORDER BY T.name ASC"

    qry = "SELECT F.fid, F.name, F.filetype, F.uid, F.gid, F.userpm, F.grppm, F.otherpm, F.mtime, F.size, F.nlink, L.linkfid"\
        " FROM (SELECT * FROM tree NATURAL JOIN fattrb) F LEFT JOIN link L ON F.fid = L.fid WHERE F.parentid = "+str(fidfolder)+" ORDER BY F.name ASC"    

    if query_execute(qry) == 0:
        result = query_fetchresult_all()
        # print(qry)
        # print("query output : ", result)        
        if (len(result) != 0):
            return result
        else:
            return -1
    else:
        return -1    

def get_file_with_attrib(fidfile):
    qry = "SELECT F.fid, F.name, F.filetype, F.uid, F.gid, F.userpm, F.grppm, F.otherpm, F.mtime, F.size, F.nlink, L.linkfid"\
        " FROM (SELECT * FROM tree NATURAL JOIN fattrb) F LEFT JOIN link L ON F.fid = L.fid"\
        " WHERE F.fid = "+str(fidfile)
    if query_execute(qry) == 0:
        result = query_fetchresult_one()
        # print(qry)
        # print("query output : ", result)        
        if (len(result) != 0):
            return result
        else:
            return -1
    else:
        return -1         

def get_linkfid_from_linkpath(fid, nodeid, resolved_link_path):
    subqry = "SELECT parentid FROM tree INNER JOIN fattrb as f WHERE fid = "+str(fid) +" AND f.nodeid = "+str(nodeid);
    print('fid '+str(fid))
    print('nodeid '+str(nodeid))
    print('rpath '+str(resolved_link_path))
    if query_execute(subqry) == 0:
       subresult = query_fetchresult_one()
       print('subresult '+str(subresult))
       pfid = subresult[0]
       print(pfid)
       fid_result = get_fid_from_dirpath(pfid, str(resolved_link_path))
       print('fid_result ' +str(fid_result))
       find_linkfid_qry = "UPDATE link SET linkfid='"+str(fid_result)+"' WHERE fid='"+str(fid)+"'"
       query_execute(find_linkfid_qry)

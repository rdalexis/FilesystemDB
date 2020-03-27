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
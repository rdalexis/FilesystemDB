import os
import mysql.connector
from mysql.connector import errorcode

import mysqlglobals as gl

def query_execute(query):
    try:
        gl.cursor.execute(query)
        gl.qryrecords = gl.cursor.fetchall()
    except mysql.connector.Error as err:
        print("Failed executing query : ", query)
        print("Error : {}".format(err))
        return 1   

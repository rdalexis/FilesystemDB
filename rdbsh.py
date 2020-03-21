# Imports
import sys, getopt
import os
import mysql.connector
from mysql.connector import errorcode

from mysqlfscreate import create_database, open_database

# Import logger module
import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Import of local files
#from mysqlfscreate import CreateFSDatabase

# Globals

# mysql connection
def OpenMysqlConn(uname, pwd, hostip, existingdbname):
   global cnx, cursor

   try: 
      cnx = mysql.connector.connect(user=uname, password=pwd, host=hostip)
   except mysql.connector.Error as err:
      logging.error(str(err))
      return 1
   else:
      cursor = cnx.cursor()
      return 0



# mysql close connection
def CloseMysqlConn():
   cursor.close()
   cnx.close()

def main(argv):
   uname = ''
   pwd = ''
   hostip = ''
   createdbname = ''
   dbcreatefilepath = ''
   existdbname = ''
   dbname = ''  
   
   #print ("Number of arguments:", len(sys.argv), "arguments.")
   #print ("Argument List:", str(sys.argv))

   if len(sys.argv) <= 1:
      logging.info ("USAGE : rdbsh.py -u <user> -p <password> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit()

   try:
      opts, args = getopt.gnu_getopt(argv, "hu:p:i:e:c:f:", ["help"])
   except getopt.GetoptError:
      logging.debug ("ERROR : USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit(2)

   logging.info("Opts : %s", opts)
   logging.info("Args : %s", args)   
      
   for opt, arg in opts:
      if opt in ("-h", "--help"):
         logging.debug ("USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
         sys.exit()
      elif opt == '-u':
         uname = arg
      elif opt == '-p':
         pwd = arg
      elif opt == '-i':
         hostip = arg
      elif opt == '-c':
         createdbname = arg
      elif opt == '-e':
         existdbname = arg
      elif opt == '-f':
         dbcreatefilepath = arg

   logging.debug ("Input parameters : ")
   logging.debug ("Username : %s", uname)
   logging.debug ("Password : %s", pwd)
   logging.debug ("Host ip : %s", hostip)
   logging.debug ("Create db name : %s", createdbname)
   logging.debug ("Existing DB name : %s", existdbname)
   logging.debug ("DB File Create Path : %s", dbcreatefilepath)

   if (uname == "" or pwd == "" or hostip == ""):
      logging.debug ("USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit(2)

   if (createdbname == "" and existdbname == ""):
      logging.info("Database parameter is missing")
      sys.exit(2)

   # open mysql connection
   if (OpenMysqlConn(uname, pwd, hostip, existdbname) != 0):
      sys.exit(2)

   # create db if required
   if (createdbname != ""):
      if (create_database(cnx, cursor, createdbname, dbcreatefilepath)):
         CloseMysqlConn()
         sys.exit(2)
      else:
         dbname = createdbname
   else:
      dbname = existdbname      
      # open database
      if (open_database(cursor, dbname)):
         CloseMysqlConn()
         sys.exit(2)     

   # test database
   logging.info("\n\nDISPLAYING ALL TABLES IN DB")
   cursor.execute("SHOW TABLES")
   for x in cursor:
      print(x)

   # Close sql connection
   CloseMysqlConn()
  
if __name__== "__main__":
    main(sys.argv[1:])
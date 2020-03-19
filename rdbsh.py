# Imports
import sys, getopt
import os
import mysql.connector
from mysql.connector import errorcode
from goto import goto, label

# Import logger module
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# Import of local files
from mysqlfscreate import CreateFSDatabase

# Globals

# mysql connection
def OpenMysqlConn(uname, pwd, hostip, existingdbname):
   global cnx, cursor

   try: 
      cnx = mysql.connector.connect(user=uname, password=pwd,
                              host=hostip, database=existingdbname)
   except mysql.connector.Error as err:
      #logging.error("Mysql connection parameters not working.")
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
   
   print ("Number of arguments:", len(sys.argv), "arguments.")
   print ("Argument List:", str(sys.argv))

   if len(sys.argv) <= 1:
      logging.debug ("USAGE : rdbsh.py -u <user> -p <password> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit()

   try:
      opts, args = getopt.gnu_getopt(argv, "hu:p:i:e:c:f:", ["help"])
   except getopt.GetoptError:
      logging.debug ("ERROR : USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit(2)

   #print(opts)
   #print(args)   
      
   for opt, arg in opts:
      if opt in ("-h", "--help"):
         logging.debug ("USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
         sys.exit()
      elif opt == '-u':
         uname = arg
      elif opt == '-p':
         pwd = arg
      elif opt == 'i':
         hostip = arg
      elif opt == '-c':
         createdbname = arg
      elif opt == '-e':
         existdbname = arg
      elif opt == 'f':
         dbcreatefilepath = arg

   logging.debug ("Input parameters : ")
   logging.debug ("Username : %s", uname)
   logging.debug ("Password : %s", pwd)
   logging.debug ("Host ip : %s", hostip)
   logging.debug ("Created db name : %s", createdbname)
   logging.debug ("Existing DB name : %s", existdbname)
   logging.debug ("DB File Create Path : %s", dbcreatefilepath)

   # open mysql connection
   if (OpenMysqlConn(uname, pwd, hostip, existdbname) != 0):
      sys.exit(2)

   # create db if required
   if (createdbname != NULL):
      if (CreateFSDatabase() != 0)

      
   cursor.execute("USE university")
   for x in cursor:
      print(x)

label: EndConnection
   # Close sql connection
   CloseMysqlConn()
  
if __name__== "__main__":
    main(sys.argv[1:])
# Imports
import sys, getopt
import os, glob
import errno
import mysql.connector
from mysql.connector import errorcode
import subprocess

import mysqlglobals as gl
from cd import cd_main
from ls import ls_main
from find import find_main
from grep import grep_main

from mysqlfscreate import create_database, open_database
import dbman

# Import logger module
import logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
#logging levels : https://docs.python.org/3/library/logging.html#levels

# Import of local files
#from mysqlfscreate import CreateFSDatabase

# Globals
# CMDS = ['cd', 'ls', 'find', 'grep']

# mysql connection
def OpenMysqlConn(uname, pwd, hostip):
   try: 
      gl.cnx = mysql.connector.connect(user=uname, password=pwd, host=hostip, use_pure=True)
   except mysql.connector.Error as err:
      logging.error(str(err))
      return 1
   else:
      gl.cursor = gl.cnx.cursor(buffered=True)
      return 0

# mysql close connection
def CloseMysqlConn():
   gl.cursor.close()
   gl.cnx.close()

def getUsersGroups():
   UserQry = "SELECT * FROM user"
   if dbman.query_execute(UserQry) == 0:
      results = dbman.query_fetchresult_all()
      for row in results:
         gl.users[row[0]] = row[1]

   GroupQry = "SELECT * FROM fgroup"
   if dbman.query_execute(GroupQry) == 0:
      results = dbman.query_fetchresult_all()
      for row in results:
         gl.groups[row[0]] = row[1]


def TerminalInit():
   # Get fid of root 
   gl.fidroot = dbman.get_childfid(0, "/", True, False)
   #print(gl.fidroot)

   # get ~ path /home/$user
   gl.fidhome = dbman.get_childfid(gl.fidroot, "home", True, False)
   # TODO : Set fid of user if more than one user
   gl.fiduser = dbman.get_childfid(gl.fidhome, "%", True, False)
   #print(gl.fidhome, gl.fiduser)
   
   gl.terminalpath = "~"
   gl.current_fid = gl.fiduser

   getUsersGroups()

   # paths
   pathdirs = gl.PATH.split(":")
   for path in pathdirs:
      if path != "":
         fid = dbman.get_fid_from_dirpath(gl.fidroot, path, True, False)
         if fid != -1:
            gl.pathfids.append(fid)

   try:
      os.mkdir("exectemp", 0o777)
   except OSError as exc:
      if exc.errno != errno.EEXIST:
         raise
      pass   
 

def checkandexecute(cmdparam):

   filefid, mode = dbman.get_fid_from_dirpath(gl.current_fid, cmdparam[0], False, True)
   if filefid == -1:
      for pathfid in gl.pathfids:
         filefid, mode = dbman.get_fid_from_dirpath(pathfid, cmdparam[0], False, True)
         if filefid != -1:
            break

   if filefid == -1:
      print(cmdparam[0], ": command not found")
      return

   # get data
   qry = "SELECT data FROM tree NATURAL JOIN fdata WHERE fid = "+str(filefid)
   if dbman.query_execute(qry) == 1:
      print(cmdparam[0], " execution error")
      return
   data = dbman.query_fetchresult_one()

   fileDir = os.path.dirname(os.path.realpath('__file__'))
   filepath = "exectemp/"+str(cmdparam[0])
   filename = os.path.join(fileDir, filepath)

   try:
      os.remove(filename)
   except OSError as exc:
      if exc.errno != errno.ENOENT:
         # remove issue
         raise
      pass        

   f = open(filename,"wb+")
   f.write(data[0])
   f.close()
   os.chmod(filename, 0o777)

   cwd = os.getcwd()
   os.chdir(cwd+"/exectemp")

   p = subprocess.Popen(cmdparam)
   p.wait()

   os.chdir(cwd)

   try:
      os.remove(filename)
   except OSError as exc:
      if exc.errno != errno.ENOENT:
         # remove issue
         raise
      pass


def main(argv):
   uname = ''
   pwd = ''
   hostip = ''
   createdbname = ''
   dbcreatefilepath = ''
   existdbname = ''
   dbname = ''

   gl.globalsInit()
   
   #print ("Number of arguments:", len(sys.argv), "arguments.")
   #print ("Argument List:", str(sys.argv))

   if len(sys.argv) <= 1:
      logging.error ("USAGE : rdbsh.py -u <user> -p <password> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit()

   try:
      opts, args = getopt.gnu_getopt(argv, "hu:p:i:e:c:f:", ["help"])
   except getopt.GetoptError:
      logging.error ("ERROR : USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit(2)

   # logging.debug("Opts : %s", opts)
   # logging.debug("Args : %s", args)   
      
   for opt, arg in opts:
      if opt in ("-h", "--help"):
         logging.error ("USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
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

   # logging.debug ("Input parameters : ")
   # logging.debug ("Username : %s", uname)
   # logging.debug ("Password : %s", pwd)
   # logging.debug ("Host ip : %s", hostip)
   # logging.debug ("Create db name : %s", createdbname)
   # logging.debug ("Existing DB name : %s", existdbname)
   # logging.debug ("DB File Create Path : %s", dbcreatefilepath)

   if (uname == "" or pwd == "" or hostip == ""):
      logging.error ("USAGE : rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>")
      sys.exit(2)
   elif (createdbname == "" and existdbname == ""):
      logging.error("Database parameter is missing")
      sys.exit(2)
   elif (createdbname != "" and dbcreatefilepath == ""):
      logging.error("Filesystem path parameter is missing")
      sys.exit(2)   

   # open mysql connection
   if (OpenMysqlConn(uname, pwd, hostip) != 0):
      sys.exit(2)

   # set the value of max_allowed_packet to 1 GB(max_limit) and reopen mysql session
   gl.cursor.execute("SET GLOBAL max_allowed_packet=1073741824")
   CloseMysqlConn()
   if (OpenMysqlConn(uname, pwd, hostip) != 0):
      sys.exit(2)

   # check if max_allowed_packet is set or not
   gl.cursor.execute("SHOW VARIABLES LIKE 'max_allowed_packet'")
   gl.qryrecords = gl.cursor
   # for x in gl.qryrecords:
   #    logging.debug(x)

   # create db if required
   if (createdbname != ""):
      if (create_database(gl.cnx, gl.cursor, createdbname, dbcreatefilepath)):
         CloseMysqlConn()
         sys.exit(2)
      else:
         dbname = createdbname
   else:
      dbname = existdbname      
      # open database
      if (open_database(gl.cnx, gl.cursor, dbname)):
         CloseMysqlConn()
         sys.exit(2)     

   # initialize terminal path
   TerminalInit()

   # display prompt
   while True:
      ui = input("mysql@mysqlserver:" + gl.terminalpath + "$ ")
      cmdparam = ui.split()
      if (len(cmdparam) == 0): continue
      if (cmdparam[0] == 'cd'):
         #print("cd Command")
         cd_main(cmdparam)
      elif(cmdparam[0] == 'ls'):
         if len(cmdparam) == 1:
            ls_main()
         elif len(cmdparam) == 2:
            if cmdparam[1] == '-l':
               ls_main(True)
            else:
               ls_main(False, cmdparam[1])
         elif len(cmdparam) == 3:
            if cmdparam[1] == '-l':
               ls_main(True, cmdparam[2])
            else:
               print("Please check the arguments given for ls command")
         else:
            print("Please check the arguments given for ls command")
      elif(cmdparam[0] == 'find'):
         #print("find Command")
         if len(cmdparam) == 1:
            find_main()
         elif len(cmdparam) == 2:
            find_main(cmdparam[1])
         elif len(cmdparam) == 3:
            if cmdparam[1] == '-name':
               find_main(None, cmdparam[2])
         elif len(cmdparam) == 4:
            if cmdparam[2] == '-name':
               find_main(cmdparam[1], cmdparam[3])
         else:
            print("Please check the arguments given for find command")
      elif(cmdparam[0] == 'grep'):
         #print("grep Command")
         options = []
         if '-n' in cmdparam:
            options.append('linenum')
            cmdparam.remove('-n')
         if len(cmdparam) == 3:
            grep_main(cmdparam[1], cmdparam[2], options) 
      elif(cmdparam[0] == 'exit'):
         # print("Terminating mysqlfs shell")
         break
      else:
         checkandexecute(cmdparam)

   # Do all necessary cleanups

   # Close sql connection
   CloseMysqlConn()
  
if __name__== "__main__":
    main(sys.argv[1:])

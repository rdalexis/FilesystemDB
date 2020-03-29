import os
import mysqlglobals as gl
import dbman

def update_terminal_path(newpath):
    pathsplit = newpath.split('/')
    #print(pathsplit)
    for i in range(len(pathsplit)):
        # Check for / at begin and end
        if pathsplit[i] == "" and i == 0:
            gl.terminalpath = ""
        elif pathsplit[i] == "~":
            gl.terminalpath = "~"
        elif pathsplit[i] == "..":
            gl.terminalpath = gl.terminalpath.rsplit("/", 1)[0]
        else:
            gl.terminalpath = gl.terminalpath+"/"+pathsplit[i]

    #print(gl.terminalpath)

def cd_main(cmdparam):

    # print (dbman.get_childfid(0, 'a.sql', False, False))
    # print (dbman.get_childfid(0, 'tmplnk', False, False))
    # print (dbman.get_childfid(0, 'tmplnk', False, True))
    # print (dbman.get_childfid(0, 'b.sql', False, True))

    # print(dbman.get_parentfid(0))
    # print(dbman.get_parentfid(14))
    # print(dbman.get_parentfid(1))
    # return

    if (len(cmdparam) > 2):
        print("bash: cd: too many arguments")
        return
    elif (len(cmdparam) == 1):
        gl.terminalpath = "~"
        # TODO : Set fid to /home/$usr
        gl.current_fid = 0
        return

    print("CD Path : ", cmdparam[1])

    if (cmdparam[1] == "/"):
        gl.terminalpath = "/"
        gl.current_fid = 0
        return
    elif (cmdparam[1] == "."):
        return

    # Args: IN current fid, IN path, OUT updated fid, OUT updated path string 
    cd_args = [gl.current_fid, cmdparam[1], 0, 0]
    result_args = dbman.call_procedure_argresults('Unix_GetFileIdFromPath', cd_args)
    print(result_args)
    if (result_args != 1):
        if (result_args[2] != -1):
            gl.current_fid = result_args[2]
            update_terminal_path(cmdparam[1])
        else:
            print("bash: cd: " + cmdparam[1] + ": No such file or directory")

    return

# print (dbman.get_childfid(0, 'a.sql', False))
# print (dbman.get_childfid(0, 'b.sql', False))
# print(dbman.get_parentfid(0))
# print(dbman.get_parentfid(14))
# print(dbman.get_parentfid(1))
# return
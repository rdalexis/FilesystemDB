import os
import mysqlglobals as gl
import dbman

def cd_main(cmdparam):

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
            gl.terminalpath = result_args[3]
        else:
            print("bash: cd: " + cmdparam[1] + ": No such file or directory")

    return
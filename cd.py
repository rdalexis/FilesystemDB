import os
import mysqlglobals as gl
import dbman

def update_terminal_path(newpath):
    pathsplit = newpath.split('/')

    #print(pathsplit)
    for i in range(len(pathsplit)):
        # Check for / at begin and end
        if pathsplit[i] == "":
            if i == 0:
                gl.terminalpath = ""
            else:
                continue
        elif pathsplit[i] == "~":
            gl.terminalpath = "~"
        elif pathsplit[i] == "..":
            if gl.terminalpath == "~":
                gl.terminalpath = "/home"
            else:
                gl.terminalpath = gl.terminalpath.rsplit("/", 1)[0]
        elif pathsplit[i] == ".":
            continue
        else:
            if gl.terminalpath == "/": 
                gl.terminalpath = "/"+pathsplit[i]
            else:
                gl.terminalpath = gl.terminalpath+"/"+pathsplit[i]

    if gl.terminalpath == "": gl.terminalpath = "/"
    #print(gl.terminalpath)

# cd ..
# cd /
# cd ~
# cd /home/mk
# cd /home/../home/..
# cd a.txt
# cd //
# cd ../../../../.. 
def cd_main(cmdparam):

    if (len(cmdparam) > 2):
        print("bash: cd: too many arguments")
        return
    elif (len(cmdparam) == 1):
        gl.terminalpath = "~"
        # TODO : Set fid to /home/$usr
        gl.current_fid = gl.fiduser
        return

    #print("CD traverse path : ", cmdparam[1])

    newfid, filetype = dbman.get_fid_from_dirpath(gl.current_fid, cmdparam[1], False, True)
    if newfid != -1:
        if filetype != 16384:
            print("bash: cd:", cmdparam[1] ,": Not a directory")
        else:
            # print("fid : ", newfid, "for ", cmdparam[1])
            update_terminal_path(cmdparam[1])
            gl.current_fid = newfid
    else:
        print("bash: cd:", cmdparam[1] ,": No such file or directory")

    return
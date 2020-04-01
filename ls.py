import os
import mysqlglobals as gl
import dbman

# ls a.txt
# ls Desktop/a.txt
# ls Desktop/
# ls -l a.txt
# ls -l Desktop/a.txt
# ls -l Desktop/
def ls_main(listdetailed = False, path = None):

    if path is not None:
        lsfid, lsmode = dbman.get_fid_from_dirpath(gl.current_fid, path, False, True)
    else:
        lsfid = gl.current_fid
        lsmode = 16384

    print(lsfid, lsmode)

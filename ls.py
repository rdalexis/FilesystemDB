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
        if lsfid == -1:
            print("ls: cannot access '{}': No such file or directory".format(path))
            return
    else:
        lsfid = gl.current_fid
        lsmode = 16384

    if (lsmode & 16384) == 16384:
        attrib = dbman.get_folder_elements_with_attrib(lsfid)
    else:
        attrib = dbman.get_file_with_attrib(lsfid)

    for row in attrib:
        print(row)

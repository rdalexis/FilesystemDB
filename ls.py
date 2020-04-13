import os
import mysqlglobals as gl
import dbman
import datetime

S_IFMT = 0o170000
S_IFREG = 0o100000
S_IFDIR = 0o40000
S_IRWXO = 0o7 
S_IROTH = 0o4
S_IWOTH = 0o2
S_IXOTH = 0o1

def getPermission(per):
    modestring = ""
    if per & S_IRWXO == S_IRWXO:
        modestring += "rwx"
    else:
        if per & S_IROTH == S_IROTH: modestring += "r"
        else: modestring += "-"
        if per & S_IWOTH == S_IWOTH: modestring += "w"
        else: modestring += "-"
        if per & S_IXOTH == S_IXOTH: modestring += "x"
        else: modestring += "-"
    return modestring

def ls_detailed(attrib = []):
# F.fid, F.name, F.filetype, F.uid, F.gid, F.userpm, F.grppm, F.otherpm, F.mtime, F.size, F.nlink, L.linkfid
    name = attrib[1]
    filetype = attrib[2]
    uid = attrib[3]
    gid = attrib[4]
    userpm = (attrib[5] >> 6) & S_IRWXO
    grppm = (attrib[6] >> 3) & S_IRWXO
    otherpm = attrib[7]
    mtime = attrib[8]
    size = attrib[9]
    nlink = attrib[10]
    linkfid = attrib[11]

    modestring = ""
    if linkfid is not None:
        modestring += "l"
    elif filetype == S_IFREG:
        modestring += "-"
    elif filetype == S_IFDIR:
        modestring += "d"
    else:
        # to handle others
        modestring += "-"

    modestring += getPermission(userpm)
    modestring += getPermission(grppm)
    modestring += getPermission(otherpm)

    user = gl.users[uid]
    group = gl.groups[gid]

    mtimestr = str(mtime.strftime("%b %d, %H:%M"))

    print(modestring, nlink, user, group, "{:>6}".format(size), mtimestr, name)

# ls a.txt
# ls Desktop/a.txt
# ls Desktop/
# ls -l a.txt
# ls -l Desktop/a.txt
# ls -l Desktop/
def ls_main(listdetailed = False, path = None):

    # handling for /

    if path is not None:
        lsfid, filetype = dbman.get_fid_from_dirpath(gl.current_fid, path, False, True)
        if lsfid == -1:
            print("ls: cannot access '{}': No such file or directory".format(path))
            return
    else:
        lsfid = gl.current_fid
        filetype = 16384

    # TODO Need to handle link
    if (filetype & 16384) == 16384:
        attribarray = dbman.get_folder_elements_with_attrib(lsfid)
        files = ""
        if listdetailed == False:
            for attrib in attribarray:
                if attrib[1] != "/":
                    files += attrib[1] +"  "
            print(files)
        else:
            for attrib in attribarray:
                if attrib[1] != "/":
                    ls_detailed(attrib)

    else:
        attrib = dbman.get_file_with_attrib(lsfid)
        filename = path.rsplit("/", 1)[-1]

        if listdetailed == False:
            print(filename)
        else:
            ls_detailed(attrib)
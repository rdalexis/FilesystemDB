import os
import mysqlglobals as gl
import dbman

def recursive_find(find_path, fileid, search_item):
    output = None
    temp_query = "SELECT fid,name,parentid FROM tree WHERE parentid="+str(fileid)+";"
    output = dbman.query_execute(temp_query)
    if output == 0:
       query_result = dbman.query_fetchresult_all()
       #print(query_result)
    for i in range(len(query_result)):
        (fid, name, parentid) = query_result[i]
        # skip to avoid infinite looping
        if fid == 0 and parentid == 0:
           continue
        new_directory = find_path+str('/')+name
        if search_item is not None:
           if search_item == name:
              print(new_directory)
              continue # skip the rest of the code and continue to the next iteration
        else:
           print(new_directory)
        recursive_find(new_directory, fid, search_item)

def form_find_query(name, parentid):
    find_query = "SELECT fid FROM tree WHERE name='"+str(name)+"' AND parentid=("+str(parentid)+")"
    return find_query

#find
#find .
#find /
#find /var/cache
#find /var/cache/
#find ./cache
#find ../var
#find ../../var
#find /var/cache -name filename.txt
#find /var/cache -name foldername
def find_main(find_path=None, search_item=None):
    # find with no parameters works on the current directory
    if find_path is None:
       find_path = '.'

    fid_found = dbman.get_fid_from_dirpath(gl.current_fid, find_path, True, False)
    """if find_path in ['.']:
       print('Get the value of current_fid from global variables')
       fid_found = gl.current_fid
    elif find_path in ['/']:
       print('Find command in root directory')
       fid_found = gl.fidroot
    else:  
        #dbman.get_fid_from_dirpath(gl.current_fid, find_path, True, False)
        # treating both "/var/cache" and "/var/cache/" equally
        find_path = find_path[:-1] if find_path.endswith('/') else find_path
        find_path_contents = find_path.split('/')
        print(find_path_contents)
        pid = None
        # first_part can be ['.','','..']
        first_part = find_path_contents[0]
        start_from = 1
        if first_part == '':
           pid = gl.fidroot
        elif first_part == '.':
           print('Get the current fid from the global variables')
           pid = gl.current_fid
        elif first_part == '..':
           print('Find the number of '+ '..' + ' in find_path_contents')
           print('Iterate as times as the number of '+ '..' + ' to find the correct fid')
        else:
           print('Please check the path of the directory')

        for i in range(start_from, len(find_path_contents)):
            pid = form_find_query(find_path_contents[i], pid)  
        pid = pid + ';'
        print(pid)
        output = None
        query_result = None
        output = dbman.query_execute(pid)
        if output == 0:
           query_result = dbman.query_fetchresult_all()
        print(query_result)
        if len(query_result) == 1:
           for row in query_result:
               fid_found = row[0]"""
    print('find_path '+str(find_path))
    print('fid_found '+str(fid_found))
   
    if search_item is None:
       print(find_path)
    if find_path in ['/', '../']:
       find_path = find_path.replace('/','') # workaround to avoid double slashes(//) while printing sub-directories paths.    
    recursive_find(find_path, fid_found, search_item)

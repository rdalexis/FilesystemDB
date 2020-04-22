import os
import mysqlglobals as gl
import dbman
from ls import *

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
         new_search = search_item.replace('*', '')        
         if new_search in name:
            print(new_directory)
            ls_detailed(dbman.get_file_with_attrib(fid))
            continue # skip the rest of the code and continue to the next iteration
      else:
         print(new_directory)
         ls_detailed(dbman.get_file_with_attrib(fid))
      recursive_find(new_directory, fid, search_item)

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
#find filename -> displays the filename if exists, else error
#find folder -> displays the folder contents if exists, else error
def find_main(find_path=None, search_item=None):
    # find with no parameters works on the current directory
    if find_path is None:
       find_path = '.'

    fid_found = dbman.get_fid_from_dirpath(gl.current_fid, find_path, True, False)
    print('find_path '+str(find_path))
    print('fid_found '+str(fid_found))
    if fid_found == -1:
       print("find: ‘"+str(find_path)+"’: No such file or directory")
       return

    if search_item is None:
       print(find_path)
       ls_detailed(dbman.get_file_with_attrib(fid_found))
    if find_path in ['/', '../']:
       find_path = find_path.replace('/','') # workaround to avoid double slashes(//) while printing sub-directories paths.    
    recursive_find(find_path, fid_found, search_item)

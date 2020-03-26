import os
import mysqlglobals as gl
import dbman

def dummy(directory, fileid):
    temp_query = "SELECT fid,name,parentid FROM tree WHERE parentid="+str(fileid)+";"
    output = dbman.query_execute(temp_query)
    for i in range(len(output)):
        (fid, name, parentid) = output[i]
        new_directory = directory+str('/')+name
        print(new_directory)
        dummy(new_directory, fid)

def form_find_query(name, parentid):
    find_query = "SELECT fid FROM tree WHERE name='"+str(name)+"' AND parentid=("+str(parentid)+")"
    return find_query

def find_main(directory, content_to_search):
    dir_contents = directory.split('/')
    print(dir_contents)
    pid = ''   
    if dir_contents[0] in ['.','']:
       pid = 0
    for i in range(1, len(dir_contents)):
        pid = form_find_query(dir_contents[i], pid)  
    pid = pid + ';'
    print(pid)
    output = dbman.query_execute(pid)
    print(output)
    if len(output) == 1:
       for row in output:
           fid_found = row[0]
    
    dummy(directory, fid_found)

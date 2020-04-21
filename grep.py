import os
import mysqlglobals as gl
import dbman

#grep directory /snap/README
#grep 'directory' /snap/README
#grep "directory" /snap/README
#grep -n 'directory' /snap/README -> line number
#grep 'dir*' /snap/README
#grep 'directory' /snap/*
#grep 'directory' /snap/READ*
#grep 'directory' /snap/ -> Is a directory
#grep 'directory' /snap/README1 -> No such file or directory
def grep_main(search_string, file_to_search, options = []):
#first check if file_to_search is a file or folder
#if folder, report error
    #print(gl.current_fid)
    search_string = search_string.replace("\"","")
    search_string = search_string.replace("\'","")
    search_string = search_string.replace('*','')
    if '*' in file_to_search:
       grep_wildcard_path_handling(search_string, file_to_search, options)
       return
    (fid_found, file_type) = dbman.get_fid_from_dirpath(gl.current_fid, file_to_search, False, True)
    #print(fid_found)
    #print(file_type)
    if file_type is not None:
       if (file_type & 16384) == 16384:
          print("grep: "+str(file_to_search) + str(": Is a directory"))
          return
    if fid_found == -1 or file_type is None:
       print("grep: "+str(file_to_search) + str(": No such file or directory"))
       return
    args = (fid_found, search_string)
    output = dbman.call_procedure_selectresults('grep', args)
    if output == 0:
       grep_sp_results = dbman.sp_fetch_selectresults_all()
       for i in range(len(grep_sp_results)):
           (line_no, line) = grep_sp_results[i]
           if 'linenum' in options:
              print(str(line_no)+str(':')+str(line))
           else:  
              print(str(line))
      
def grep_wildcard_path_handling(search_string, file_to_search, options = []):
    file_path_split = file_to_search.rsplit("/", 1)
    #print(file_path_split)
    if (len(file_path_split) == 2):
       (fid_found, file_type) = dbman.get_fid_from_dirpath(gl.current_fid, file_path_split[0], False, True)
       #print(fid_found)
       #print(file_type)
       matching_items = dbman.get_matching_elements_in_folder(fid_found, file_path_split[1])
       if matching_items == -1:
          return
       
       separate_files = matching_items.copy()
       # considering folders first 
       for i in range(len(matching_items)):
           (fid, name, filetype) = matching_items[i]
           if fid == fid_found:
              separate_files.remove((fid, name, filetype))
              continue
           #print(fid)
           #print(name)
           #print(filetype)
           if filetype is not None:
              if (filetype & 16384) == 16384:
                 print("grep: "+file_path_split[0]+"/"+str(name) + str(": Is a directory"))
                 separate_files.remove((fid, name, filetype))
        
        # considering files next        
       for j in range(len(separate_files)):
           (fid, name, filetype) = separate_files[j]
           args = (fid, search_string)
           output = dbman.call_procedure_selectresults('grep', args)
           if output == 0:
              grep_sp_results = dbman.sp_fetch_selectresults_all()
              for i in range(len(grep_sp_results)):
                  (line_no, line) = grep_sp_results[i]
                  #print(line_no)
                  #print(line)
                  if 'linenum' in options:
                     print(file_path_split[0]+"/"+str(name)+str(':')+str(line_no)+str(':')+str(line))
                  else:  
                     print(file_path_split[0]+"/"+str(name)+str(':')+str(line))

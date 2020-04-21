import os
import mysqlglobals as gl
import dbman

#grep directory /snap/README
#grep 'directory' /snap/README
#grep "directory" /snap/README
#grep -i "directory" /snap/README -> ignore case
#grep -n 'directory' /snap/README -> line number
def grep_main(search_string, file_to_search, options = []):
#first check if file_to_search is a file or folder
#if folder, report error
    #print(gl.current_fid)
    search_string = search_string.replace("\"","")
    search_string = search_string.replace("\'","")
    (fid_found, file_type) = dbman.get_fid_from_dirpath(gl.current_fid, file_to_search, False, True)
    if file_type is not None:
       if (file_type & 16384) == 16384:
          print("grep: "+str(file_to_search) + str(": Is a directory"))
          return
    else:
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

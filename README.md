# FilesystemDB
Modeling a file system in a MYSQL database

# Usage
sudo python3 rdbsh.py -u <user> -p <password> -i <host ip> -e <exising db name> -c <create db name> -f <filesystem path>"
Options:
-u: Username of the MySQL server
-p: Password of the MySQL server
-i: IP address of the machine where MySQL server is running
-e: Name of the existing database that was already created
-c: Name of the database to be created
-f: Path of the filesystem

# Sample inputs
# Creates database from the filesystem path and then starts the shell program
sudo python3 rdbsh.py -u root -p root -i 127.0.0.1 -c filesystem -f . 

# Makes use of already created database and then starts the shell program
sudo python3 rdbsh.py -u root -p root -i 127.0.0.1 -e filesystem 

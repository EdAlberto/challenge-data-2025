# challenge-data-2025

This repository is a code challenge. The programming language is Python.

It has the scope to create a REST API to load data in a SQLite database tables from csv files: hired_employees, departments and jobs. Also is mandatory to create a log file with inserts failure logs, generate backups in
.AVRO format and creating a process to load data in a table from certain backup .AVRO file.

The files are:

- db.py : Where the SQLite database and the three tables are created
- api_process.py : Where the REST API is created with Flask for loading data in the three tables, create the backups and logging.
- insert_data_backup.py : Where the API is invoqued and executes the processes to load the data in the three tables (taking into account logging) from three csv files and generate the backups in .AVRO format.
- restore_tables.py : Where the restore backups API is created in specific for employees table.
- upload_backup.py : Where the restore backup API is invoqued and load backup data in the employees table.

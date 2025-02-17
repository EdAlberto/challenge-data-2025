import sqlite3
from sqlite3 import Error

# Create connection and database in SQLite
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
    except Error as e:
        print(e)
    finally:
        if conn:
            conn.close()


if __name__ == '__main__':
    create_connection("db_api_challenge.db")

#Satablish connection to the database
connection_obj = sqlite3.connect('db_api_challenge.db')
cursor_obj = connection_obj.cursor()

# Create three required tables
table = """ CREATE TABLE DEPARTMENTS (
            ID INT NOT NULL,
            DEPARTMENT VARCHAR(100) NOT NULL
        );"""

table1 = """ CREATE TABLE JOBS (
            ID INT NOT NULL,
            JOB VARCHAR(100) NOT NULL
        );"""

table2 = """ CREATE TABLE EMPLOYEES (
            ID INT NOT NULL,
            NAME VARCHAR(100) NOT NULL,
            DATETIME DATETIME NOT NULL,
            DEPARTMENT_ID INT NOT NULL,
            JOB_ID INT NOT NULL
        );"""

cursor_obj.execute(table)
cursor_obj.execute(table1)
cursor_obj.execute(table2)
 
print("Tables created!!!")

connection_obj.close()
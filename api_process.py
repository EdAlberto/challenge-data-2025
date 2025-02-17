import sqlite3
from flask import Flask, request, jsonify 
from flask_cors import CORS
import pandas as pd
from json import loads
import logging
import os
from datetime import datetime
import fastavro
import time

backup_dir = "backups"
os.makedirs(backup_dir, exist_ok=True)

#Connect to the database
def connect_to_db():
    conn = sqlite3.connect('db_api_challenge.db', check_same_thread=False)
    return conn

# Configure logging for insert errors
logging.basicConfig(filename="data_insert_errors.log", level=logging.ERROR, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

#Insert data in departments table from json data
def insert_departments(department,retries=5, delay=1):
    inserted_user = {}
    for attempt in range(retries):
        try:
            conn = connect_to_db()
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("INSERT INTO DEPARTMENTS (id, department) VALUES (?, ?)", (department['id'],   
                        department['department']) )
            conn.commit()
            conn.close()
            return inserted_user
        
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < retries - 1:
                    logging.warning(f"Database locked. Retrying {attempt + 1}/{retries}...")
                    time.sleep(delay)  # Wait and retry
            else:
                logging.error(f"Data integrity error: {e} - Data: {department['id']}, {department['department']}")
                return False
        
        except sqlite3.OperationalError as e:
            logging.error(f"Database error: {e} - Data: {department['id']}, {department['department']}")
            return False
        
        finally:
            if conn:
                conn.close()  # Ensure connection always closes

#Insert data in jobs table from json data
def insert_jobs(jobs, retries=5, delay=1):
    inserted_user = {}
    for attempt in range(retries):
        try:
            conn = connect_to_db()
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("INSERT INTO JOBS (id, job) VALUES (?, ?)", (jobs['id'],   
                        jobs['job']) )
            conn.commit()
            conn.close()
            return inserted_user
        
        except sqlite3.IntegrityError as e:
            logging.error(f"Data integrity error: {e} - Data: {jobs['id']}, {jobs['job']}")
            return False
        
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < retries - 1:
                    logging.warning(f"Database locked. Retrying {attempt + 1}/{retries}...")
                    time.sleep(delay)  # Wait and retry
            else:
                    logging.error(f"Database error: {e} - Data: {jobs['id']}, {jobs['job']}")
                    return False               
        finally:
            if conn:
                conn.close()  # Ensure connection always closes

#Insert data in employees table from json data
def insert_employees(employees,retries=5, delay=1):
    inserted_user = {}

    for attempt in range(retries):
        try:
            conn = connect_to_db()
            cur = conn.cursor()
            cur.execute("PRAGMA journal_mode=WAL;")
            cur.execute("INSERT INTO EMPLOYEES (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", (employees['id'],   
                        employees['name'], employees['datetime'], employees['department_id'],   
                        employees['job_id']) )
            conn.commit()
            conn.close()
            return inserted_user
        
        except sqlite3.IntegrityError as e:
            logging.error(f"Data integrity error: {e} - Data: {employees['id']}, {employees['name']}, {employees['datetime']}, {employees['department_id']}, {employees['job_id']}")
            return False
        
        except sqlite3.OperationalError as e:
            if "database is locked" in str(e) and attempt < retries - 1:
                    logging.warning(f"Database locked. Retrying {attempt + 1}/{retries}...")
                    time.sleep(delay)  # Wait and retry
            else:
                logging.error(f"Database error: {e} - Data: {employees['id']}, {employees['name']}, {employees['datetime']}, {employees['department_id']}, {employees['job_id']}")
                return False
            
        finally:
            if conn:
                conn.close()  # Ensure connection always closes


# Function to backup SQLite table departments to Avro file
def backup_to_avro_departments():
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        
        # Fetch all data from the table
        cur.execute(f"SELECT * FROM DEPARTMENTS")
        rows = cur.fetchall()
        
        # Define Avro schema
        avro_schema = {
            "type": "record",
            "name": "departments",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "department", "type": "string"} 
            ]
        }
        
        # Define backup file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"DEPARTMENTS_backup_{timestamp}.avro")
        
        # Write to Avro file
        with open(backup_file, "wb") as avro_file:
            fastavro.writer(avro_file, avro_schema, 
                            [{"id": row[0], "department": row[1]} for row in rows])
        
        conn.close()
        return backup_file  # Return path of the backup file

    except Exception as e:
        logging.error(f"Backup error: {e}")
        return None

    finally:
        if conn:
            conn.close()  # Ensure connection always closes

# Function to backup SQLite table jobs to Avro file
def backup_to_avro_jobs():
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        
        # Fetch all data from the table
        cur.execute(f"SELECT * FROM JOBS")
        rows = cur.fetchall()
        
        # Define Avro schema
        avro_schema = {
            "type": "record",
            "name": "jobs",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "job", "type": "string"}
            ]
        }
        
        # Define backup file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"JOBS_backup_{timestamp}.avro")
        
        # Write to Avro file
        with open(backup_file, "wb") as avro_file:
            fastavro.writer(avro_file, avro_schema, 
                            [{"id": row[0], "job": row[1]} for row in rows])
        
        conn.close()
        return backup_file  # Return path of the backup file

    except Exception as e:
        logging.error(f"Backup error: {e}")
        return None
    
    finally:
        if conn:
            conn.close()  # Ensure connection always closes

# Function to backup SQLite table employees to Avro file
def backup_to_avro_employees():
    try:
        conn = connect_to_db()
        cur = conn.cursor()
        
        # Fetch all data from the table
        cur.execute(f"SELECT * FROM EMPLOYEES")
        rows = cur.fetchall()
        
        # Define Avro schema
        avro_schema = {
            "type": "record",
            "name": "employees",
            "fields": [
                {"name": "id", "type": "int"},
                {"name": "name", "type": "string"},
                {"name": "datetime", "type": "string"},
                {"name": "department_id", "type": "int"},
                {"name": "job_id", "type": "int"} 
            ]
        }
        
        # Define backup file path
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_file = os.path.join(backup_dir, f"EMPLOYEES_backup_{timestamp}.avro")
        
        # Write to Avro file
        with open(backup_file, "wb") as avro_file:
            fastavro.writer(avro_file, avro_schema, 
                            [{"id": row[0], "name": row[1], "datetime": row[2], "department_id": row[3], "job_id": row[4]} for row in rows])
        
        conn.close()
        return backup_file  # Return path of the backup file

    except Exception as e:
        logging.error(f"Backup error: {e}")
        return None
               
    finally:
        if conn:
            conn.close()  # Ensure connection always closes

#Set flask app to deploy REST API
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

#Create api for departments to insert data in departments table
@app.route('/api/departments/add',  methods = ['POST'])
def api_add_departments():
    department = request.get_json()
    return jsonify(insert_departments(department))

#Create api to trigger departments backup
@app.route("/backups/departments", methods=["GET"])
def trigger_departments_backup():
    backup_file = backup_to_avro_departments()
    if backup_file:
        return jsonify({"message": "Backup successful", "file": backup_file}), 200
    else:
        return jsonify({"error": "Backup failed"}), 500


#Create api for jobs to insert data in jobs table
@app.route('/api/jobs/add',  methods = ['POST'])
def api_add_jobs():
    jobs = request.get_json()
    return jsonify(insert_jobs(jobs))

#Create api to trigger jobs backup
@app.route("/backups/jobs", methods=["GET"])
def trigger_jobs_backup():
    backup_file = backup_to_avro_jobs()
    if backup_file:
        return jsonify({"message": "Backup successful", "file": backup_file}), 200
    else:
        return jsonify({"error": "Backup failed"}), 500
    

#Create api for employees to insert data in employees table
@app.route('/api/employees/add',  methods = ['POST'])
def api_add_emploeyees():
    employees = request.get_json()
    return jsonify(insert_employees(employees))

#Create api to trigger employees backup
@app.route("/backups/employees", methods=["GET"])
def trigger_departments_employees_backup():
    backup_file = backup_to_avro_employees()
    if backup_file:
        return jsonify({"message": "Backup successful", "file": backup_file}), 200
    else:
        return jsonify({"error": "Backup failed"}), 500


#Run app
if __name__ == "__main__":
    app.run(debug=True, threaded=True) 
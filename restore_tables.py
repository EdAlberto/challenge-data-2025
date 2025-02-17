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

#Restore employees table

backup_dir = "backups"
os.makedirs(backup_dir, exist_ok=True)

#Connect to the database
def connect_to_db():
    conn = sqlite3.connect('db_api_challenge.db', check_same_thread=False)
    return conn

# Configure logging for insert errors
logging.basicConfig(filename="data_insert_errors_backup.log", level=logging.ERROR, 
                    format="%(asctime)s - %(levelname)s - %(message)s")

# Function to insert data into SQLite
def insert_data(records):
    conn = connect_to_db()
    cur = conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL;")

    inserted = 0
    for record in records:
        try:
            cur.execute("INSERT INTO EMPLOYEES (id, name, datetime, department_id, job_id) VALUES (?, ?, ?, ?, ?)", (record['id'],   
                        record['name'], record['datetime'], record['department_id'],   
                        record['job_id']) )
            inserted += 1
        except sqlite3.IntegrityError as e:
            logging.error(f"Integrity error: {e} - Data: {record}")
        except sqlite3.OperationalError as e:
            logging.error(f"Database error: {e} - Data: {record}")
    
    conn.commit()
    conn.close()
    return inserted

#Set flask app to deploy REST API
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

# Flask API to restore data from Avro file
@app.route("/restore", methods=["POST"])
def restore_table():
    if "file" not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files["file"]
    file_path = os.path.join("backups", file.filename)

    # Save the uploaded Avro file
    file.save(file_path)

    # Read and restore Avro data
    try:
        with open(file_path, "rb") as avro_file:
            reader = fastavro.reader(avro_file)
            records = [record for record in reader]  # Read all records

        if not records:
            return jsonify({"error": "Avro file contains no data"}), 400
        
        inserted_count = insert_data(records)
        return jsonify({
            "message": f"Restore successful: {inserted_count} rows inserted"
        }), 200

    except Exception as e:
        logging.error(f"Restore failed: {e}")
        return jsonify({"error": "Restore failed"}), 500

#Run app
if __name__ == "__main__":
    app.run(debug=True, threaded=True) 
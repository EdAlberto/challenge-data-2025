import sqlite3
from flask import Flask, request, jsonify 
from flask_cors import CORS
import pandas as pd
from json import loads
from datetime import datetime



#Connect to the database
def connect_to_db():
    conn = sqlite3.connect('db_api_challenge.db', check_same_thread=False)
    return conn

#Get results queries challenge 2

#1 Number of employees hired for each job and department in 2021 divided by quarter. The
#table must be ordered alphabetically by department and job.
def get_query1():
    try:
        conn = connect_to_db()
        df_query1 = pd.read_sql_query("SELECT d.department, j.job, case when 0 + strftime('%m', e.datetime) between  1 and  3 then count(e.id) end as Q1,case when 0 + strftime('%m', e.datetime) between  4 and  6 then count(e.id) end as Q2, case when 0 + strftime('%m', e.datetime) between  7 and  9 then count(e.id) end as Q3, case when 0 + strftime('%m', e.datetime) between  10 and  12 then count(e.id) end as Q4 FROM EMPLOYEES e JOIN DEPARTMENTS d ON e.department_id = d.id JOIN JOBS j ON e.job_id = j.id WHERE strftime('%Y',e.datetime) = '2021' GROUP BY d.department, j.job ORDER BY d.department, j.job ASC", conn)
        df_query1j = df_query1.to_json(orient="records")
        df_query1jl = loads(df_query1j)
    except:
        df_query1jl = []

    conn.close() 
    return df_query1jl

#2  List of ids, name and number of employees hired of each department that hired more
#employees than the mean of employees hired in 2021 for all the departments, ordered
#by the number of employees hired (descending).
def get_query2():
    try:
        conn = connect_to_db()
        df_query3 = pd.read_sql_query("SELECT AVG(co_ids) FROM (SELECT  count(e.id) co_ids FROM EMPLOYEES e WHERE strftime('%Y',e.datetime) = '2021' GROUP BY e.department_id )", conn)
        avg = df_query3['AVG(co_ids)']
        df_query2 = pd.read_sql_query("SELECT e.id, e.name, d.department ,query2.hired HIRED FROM EMPLOYEES e JOIN (SELECT count(e.id) hired, e.department_id FROM EMPLOYEES e JOIN DEPARTMENTS d ON e.department_id = d.id GROUP BY d.department HAVING hired > " + str(avg[0]) + ") query2 ON e.department_id = query2.department_id JOIN DEPARTMENTS d ON e.department_id = d.id ORDER BY hired DESC", conn)
        df_query2j = df_query2.to_json(orient="records")
        df_query2jl = loads(df_query2j)

    except:
        df_query2jl = []

    conn.close() 
    return df_query2jl

#Set flask app to deploy REST API
app = Flask(__name__)
CORS(app, resources={r"/*": {"origins": "*"}})

#Craete endpoint for query 1
@app.route('/api/get_query1', methods=['GET'])
def api_get_get_query1():
    return jsonify(get_query1())

#Craete endpoint for query 2
@app.route('/api/get_query2', methods=['GET'])
def api_get_get_query2():
    return jsonify(get_query2())

#Run app
if __name__ == "__main__":
    app.run(debug=True, threaded=True) 
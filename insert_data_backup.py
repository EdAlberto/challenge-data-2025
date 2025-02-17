import pandas as pd
from json import loads
import requests

#Read data from three csv files
departments = pd.read_csv('data/departments.csv',names=["id", "department"])
hired_employees = pd.read_csv('data/hired_employees.csv',names=["id", "name","datetime","department_id","job_id"])
jobs = pd.read_csv('data/jobs.csv',names=["id", "job"])

#Cast from dataframe to JSON structure
departments_r = departments.to_json(orient="records")
hired_employees_r = hired_employees.to_json(orient="records")
jobs_r = jobs.to_json(orient="records")

departments_parsed = loads(departments_r)
hired_employees_parsed = loads(hired_employees_r)
jobs_parsed = loads(jobs_r)

#print(departments_parsed)

#Insert data in the database and Backup the database tables by API REST 


for i in departments_parsed:
    requests.post('http://127.0.0.1:5000/api/departments/add', json=i)

requests.get('http://127.0.0.1:5000/backups/departments')

for i in hired_employees_parsed:
    requests.post('http://127.0.0.1:5000/api/employees/add', json=i)

requests.get('http://127.0.0.1:5000/backups/employees')

for i in jobs_parsed:
    requests.post('http://127.0.0.1:5000/api/jobs/add', json=i)

requests.get('http://127.0.0.1:5000/backups/jobs')

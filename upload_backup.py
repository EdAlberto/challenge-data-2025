import requests

url = "http://127.0.0.1:5000/restore"
file_path = "backups/EMPLOYEES_backup_20250216_205800.avro"

with open(file_path, "rb") as file:
    files = {"file": file}
    response = requests.post(url, files=files)

print("Status Code:", response.status_code)
print("Response JSON:", response.json())
import requests

# Get queries results from section 2 from endpoints

#Query 1
query1 = requests.get('http://127.0.0.1:5000/api/get_query1').json()
print(query1)

print("---------------------------------------------------------------------------------")

#Query 2
query2 = requests.get('http://127.0.0.1:5000/api/get_query2').json()
print(query2)
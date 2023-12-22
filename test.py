import requests
import json


ip = "10.0.19.10"
user = "diuser"
passwd = "svtest@pass123"

url = "https://" + ip + ":8080/session/1/session"

payload = json.dumps({
  "username": user,
  "password": passwd,
  "services": [
    "platform",
    "namespace",
    "remote-service"
  ]
})
headers = {
  'Content-Type': 'application/json'
}

session = requests.Session()

response = session.post(url, headers=headers, data=payload, verify=False)

print(response.text)

import pdb;pdb.set_trace()
url = "https://"+ip+":8080/platform"


payload = ""
headers = {
  'Content-Type': 'application/json',
  'X-CSRF-Token':  response.cookies['isicsrf'], 
  'Origin': 'https://'+ip+':8080/'
}

session.headers['Origin'] = 'https://' + ip + ':8080'


response = session.get(url, headers=headers, cookies=session.cookies,  data=payload, verify=False)
with open("/tmp/test.txt", "w") as f:
    f.write(response.text)
print(response.text)


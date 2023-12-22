import requests
import json

url = "https://10.124.114.70:8080/session/1/session"

payload = json.dumps({
  "username": "root",
  "password": "a",
  "services": [
    "platform",
    "namespace",
    "remote-service"
  ]
})
headers = {
  'Content-Type': 'application/json'
}

response = requests.request("POST", url, headers=headers, data=payload)

print(response.text)


import requests
import json

url = "https://10.124.114.70:8080/platform/2/cluster/external-ips"

payload = ""
headers = {
  'Content-Type': 'application/json',
  'X-CSRF-Token': '11defe51-5296-4e9a-8d1f-f5127b8806ed',
  'Origin': 'https://10.124.114.70:8080/',
  'Cookie': 'isicsrf=66eaa8f6-2c16-4a3b-ac18-f4d3f7542338; path=/; HttpOnly; Secure; SameSite=strict; isicsrf=11defe51-5296-4e9a-8d1f-f5127b8806ed; isisessid=eyJhbGciOiJQUzUxMiJ9.eyJhdWQiOlsicGxhdGZvcm0iLCJuYW1lc3BhY2UiLCJyZW1vdGUtc2VydmljZSJdLCJleHAiOjE3MDMyMjQyMjgsImlhdCI6MTcwMzIwOTgyOCwib25lZnMvY3NyZiI6IjExZGVmZTUxLTUyOTYtNGU5YS04ZDFmLWY1MTI3Yjg4MDZlZCIsIm9uZWZzL2lwIjoiMTAuODQuMTU3LjcxIiwib25lZnMvbm9uY2UiOjU4NTk2MTU2NjU1NjcyNTAzNSwib25lZnMvc2Vzc2lvbiI6ImUxYTc1OGU0LWY1N2UtNDVmNi1hNDEyLWUzZWQyMDJjZjdmZSIsIm9uZWZzL3VhIjoiUG9zdG1hblJ1bnRpbWUvNy4zNi4wIiwib25lZnMvemlkIjoxLCJzdWIiOiJyb290In0K.XYtgxqf4gPvfYn9wTe3N6K63Cxn0RnScWlD--2FPbFrjwnxjmnLg27VQDlP4g-gGZz5mf53lRCYP9RX7d8vOHZazvSBbnfrvMokmZFpTO2Qcu1ziQPQKx2NqBmIjYc9_p7mgWYuSiCn9SzAf2lgLPqhxtKxRV0I3-HOEU_HD2DqWRcY3B4uXHSA1Ou3UU11n0QY5KhTOfSb3QdJe0f1ZogBcv3ouv6SKG4Lvzp0U8k0bQ_ZOfpRkUv9R0xVDJ3M_VE_TG2kpf08naaLGdwMtAps-utGR1w3oTPLCGV9uKRdd5sjMErOlfDBKcO48IK1aaa05YWV-Z5DbDq_XeDO8OQIA-YElB1ABdQRPCpYPDrWiJ5HkBI_e0ThZ2GzDjf5B5-_wBIhByJY-LYS90VPu7C3OBJ4KwsakyNiJl4zLLDLyowzRPa8uSat1dGu6KXsqLv0Dj_vfC4gGyB4MgTxwwzgNJFiL7KWfFCQu9Eq0ziVZT4v1H5t7LaGAhotORLtDIyh_XsIDI8pgIYiixTehICVtl9y_VE_W-nTrS5O8bAjcLxH2-M_NJcl1Pa8huKF41sJnorC8O8be3BqiuZLJ5OIUASNabf_FQG6NwBqGyE15azBqyDdGDWm--kAy41Gf8X7bFod_MUaHzRD0JX5XUnSpKsYM9yhpIVTP9iabrT0%3D'
}

response = requests.request("GET", url, headers=headers, data=payload)

print(response.text)


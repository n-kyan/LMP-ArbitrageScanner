import requests
import pandas


api_key = "05245f8dfe7d4766966e08f9004305b7"
response = requests.get(
    "https://api.pjm.com/api/v1/rt_hrl_lmps",
    params={
        'datetime_beginning_ept': '1/1/2024 00:00to1/1/2024 23:00',
        'pnode_id': '48592',
        'rowCount': 10,
        'startRow': 1
    },
    headers={'Ocp-Apim-Subscription-Key': api_key}
)

print(f"Status: {response.status_code}")
print(f"Response: {response.text}")  # ← Print the error message
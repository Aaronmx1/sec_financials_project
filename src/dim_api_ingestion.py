# Standard library imports
import os
import json

# Third-party imports
import pandas as pd
import requests as re   # API calls
from dotenv import load_dotenv

# Load variables from .env
load_dotenv()

# Define customer headers
## A User-Agent dictionary header must be created for the SEC server to indentify my script, otherwise request will be rejected as 403 error code
headers = {'User-Agent': os.getenv("HEADER")}

# CIK number for entity
cik = 'CIK0000002488'

# Pass headers with GET request
r = re.get("https://data.sec.gov/submissions/" + cik + ".json", headers=headers)

# Validate status code
if(r.status_code==200):
    print("Status Code: ", r.status_code)
    data = r.json()
    filepath = os.getenv("BRONZE_DATA")
    with open(os.path.join(filepath,cik + "_submission.json"), "w") as f:
        json.dump(data,f)
else:
    # Error message
    print("Error Status Code: ", r.status_code)
    print("Reason: ", r.reason)
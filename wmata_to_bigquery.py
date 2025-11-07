import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import zoneinfo
import json
from google.oauth2 import service_account

API_KEY = os.environ.get('WMATA_API_KEY')
PROJECT_ID = "wmata-tracker"
DATASET_ID = "wmata_data"
TABLE_ID = "train_predictions"

# Load credentials from environment variable (it's JSON as a string)
creds_json = os.environ.get('GOOGLE_CREDENTIALS')
if creds_json: 
    credentials_info = json.loads(creds_json)
    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    client = bigquery.Client(credentials=credentials, project=PROJECT_ID)
    print("BigQuery client project:", client.project)
    print("Authenticated service account:", credentials.service_account_email)

else: 
    #Fallback that uses local gcloud auth
    client = bigquery.Client()

url = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
headers={"api_key": API_KEY}


def fetch_data():
    print("Fetching data from WMATA API...")    
    resp = requests.get(url, headers=headers)
    print("Response status:", resp.status_code)

    if resp.status_code != 200:
        print("Error:", resp.text)
        return None
    data = resp.json()    
    trains = resp.json().get("Trains", [])
    if not trains:
        print("No trains found")
        return None

    print(f"Received {len(trains)} trains")
    if not trains:
        print("No trains returned! Response:", json.dumps(data, indent=2))
        return None
    df = pd.DataFrame(trains)
    df["timestamp"] = datetime.now(zoneinfo.ZoneInfo("America/New_York"))
    return df

def bigquery_upload(df):
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    upload = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    upload.result()
    print(f"Uploaded {len(df)} rows to {table_ref} at {datetime.now(zoneinfo.ZoneInfo("America/New_York"))}")

if __name__ == "__main__":
    df = fetch_data()
    if df is not None:
        bigquery_upload(df)


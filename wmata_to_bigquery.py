import os
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import zoneinfo

API_KEY = os.environ.get('WMAT_API_KEY')
PROJECT_ID = "wmata-tracker"
DATASET_ID = "wmata_data"
TABLE_ID = "train_predictions"

client = bigquery.Client(project=PROJECT_ID)
url = "https://api.wmata.com/StationPrediction.svc/json/GetPrediction/All"
headers={"api_key": API_KEY}


def fetch_data():
    resp = requests.get(url, headers=headers)
    trains = resp.json().get("Trains", [])
    if not trains:
        print("No trains found")
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

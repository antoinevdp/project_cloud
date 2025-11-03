import json

import boto3
import os
import requests

from datetime import datetime
from dotenv import load_dotenv

from project_cloud.utils.utils_s3 import (
    upload_to_s3
)

load_dotenv()
SNCF_API_KEY = os.getenv('SNCF_API_KEY')

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "departures"

CURRENT_DATE = datetime.today().strftime('%Y%m%d')

def get_departures():
    url = "https://api.navitia.io/v1/coverage/sncf/stop_areas/stop_area%3ASNCF%3A87723197/departures?"
    headers = {'Authorization': SNCF_API_KEY}
    payload = {
        "from_datetime": CURRENT_DATE,
        "count":1000,
        "data_freshness": "base_schedule",
    }
    response = requests.get(url,headers=headers,params=payload)
    return response.json()

def main():
    json_data = get_departures()
    ingestion_timestamp = datetime.now()
    key = f"{s3_folder}/{ingestion_timestamp}/departures.json"
    upload_to_s3(s3, json_data, BUCKET_NAME, key)

if __name__ == "__main__":
    main()


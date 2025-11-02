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

CURRENT_DATE = datetime.today().strftime('%Y%m%d')

def save_local_json(json_data, filename):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', CURRENT_DATE)
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, filename)
    with open(path, 'w') as outfile:
        json.dump(json_data, outfile, indent=4)

def get_departures():
    url = "https://api.navitia.io/v1/coverage/sncf/stop_areas/stop_area%3ASNCF%3A87723197/departures?"
    headers = {'Authorization': SNCF_API_KEY}
    payload = {
        "from_datetime": CURRENT_DATE,
        "count":10,
        "data_freshness": "base_schedule",
    }
    response = requests.get(url,headers=headers,params=payload)
    return response.json()

def main():
    json_data = get_departures()
    save_local_json(json_data, f"departures.json")
    key = f"sncf/{CURRENT_DATE}/departures.json"
    upload_to_s3(s3, json_data, key, BUCKET_NAME)

if __name__ == "__main__":
    main()


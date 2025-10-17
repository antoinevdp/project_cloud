import json

import boto3
import os
import requests

from datetime import datetime
from dotenv import load_dotenv

load_dotenv()
SNCF_API_KEY = os.getenv('SNCF_API_KEY')

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"

CURRENT_DATE = datetime.today().strftime('%Y%m%d')

def upload_to_s3(json_data, filename):
    key = f"sncf/{CURRENT_DATE}/{filename}"
    try:
        s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json.dumps(json_data))
        print(f"Uploaded {key} to {BUCKET_NAME}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def save_local_json(json_data, filename):
    data_dir = os.path.join(os.path.dirname(__file__), '..', 'data', CURRENT_DATE)
    os.makedirs(data_dir, exist_ok=True)
    path = os.path.join(data_dir, filename)
    with open(path, 'w') as outfile:
        json.dump(json_data, outfile)

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
    upload_to_s3(json_data, f"departures.json")

if __name__ == "__main__":
    main()


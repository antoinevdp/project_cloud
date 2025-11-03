import boto3
import base64
import json
import os
import requests
from decimal import Decimal

from dotenv import load_dotenv
from project_cloud.utils.utils_s3 import (
    upload_to_s3
)
from datetime import datetime

load_dotenv()
USERNAME = os.getenv('OPEN_DATA_LYON_USERNAME')
PASSWORD = os.getenv('OPEN_DATA_LYON_PASSWORD')

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "parking"

def get_parkings():
    base64string = base64.b64encode(f"{USERNAME}:{PASSWORD}".encode()).decode()

    url = 'https://data.grandlyon.com/geoserver/metropole-de-lyon/ows'
    headers = {'Authorization': f"Basic {base64string}"}
    payload = {
        "SERVICE": "WFS",
        "VERSION": "2.0.0",
        "request": "GetFeature",
        "typeName": "metropole-de-lyon:parkings-de-la-metropole-de-lyon-disponibilites-temps-reel-v2",
        "outputFormat": "application/json",
        "SRSName": "EPSG:4171",
        "startIndex": "0",
        "count": 100,
        "sortBy": "gid"
    }
    response = requests.get(url, headers=headers, params=payload)
    return response.json()


def main():
    response = get_parkings()

    ingestion_timestamp = datetime.now()
    upload_to_s3(s3, response,BUCKET_NAME, f"{s3_folder}/{ingestion_timestamp}/parkings.json")


if __name__ == "__main__":
    main()

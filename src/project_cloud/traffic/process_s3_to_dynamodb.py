import boto3
from dotenv import load_dotenv
import os
from datetime import datetime, timezone

from project_cloud.utils.utils_dynamodb import get_last_processed_timestamp, batch_put_items_to_dynamodb
from project_cloud.utils.utils_s3 import get_s3_object_keys, get_json_from_s3

load_dotenv()

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "traffic"

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = "traffic"

def main():
    last_processed_timestamp_str = get_last_processed_timestamp(dynamodb, DYNAMODB_TABLE_NAME)

    keys = get_s3_object_keys(s3, BUCKET_NAME, s3_folder)

    new_keys = []
    if last_processed_timestamp_str:
        # Convert the numerical Unix timestamp from DynamoDB to a datetime object
        # Assuming it's in microseconds (adjust if using milliseconds or seconds)
        last_processed_timestamp_dt = datetime.fromtimestamp(float(last_processed_timestamp_str) / 1_000_000)

        for key in keys:
            timestamp_str_from_key = key.split('/')[1]
            # Convert the S3 key timestamp string to a timezone-aware datetime object
            timestamp_from_key_dt = datetime.fromisoformat(timestamp_str_from_key)

            if timestamp_from_key_dt > last_processed_timestamp_dt:
                new_keys.append(key)
    else:
        new_keys = keys

    if not new_keys:
        print("No new data to process.")
        return

    all_items_to_put = []

    for key in new_keys:
        response = get_json_from_s3(s3, BUCKET_NAME, key)
        ingestion_timestamp_str = key.split('/')[1] # This is the ISO string from the S3 key
        if response:
            features = response.get("features",[])
            values_list = []
            for feature in features:
                value = feature.get("properties",{})
                coordinates = feature.get("geometry",{}).get("coordinates",[])
                value["coordinates"] = coordinates
                value["ingestion_timestamp"] = int(datetime.fromisoformat(ingestion_timestamp_str).timestamp() * 1_000_000)
                value["type"] = "traffic"
                values_list.append(value)

            all_items_to_put.extend(values_list)

    if all_items_to_put:
        print(f"Attempting to batch put {len(all_items_to_put)} items to {DYNAMODB_TABLE_NAME}")
        batch_put_items_to_dynamodb(dynamodb, DYNAMODB_TABLE_NAME, all_items_to_put)
    else:
        print("No new data to process.")

if __name__ == "__main__":
    main()
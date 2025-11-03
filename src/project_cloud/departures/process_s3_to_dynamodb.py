import boto3
from dotenv import load_dotenv
from datetime import datetime

from project_cloud.utils.utils_dynamodb import get_last_processed_timestamp, batch_put_items_to_dynamodb
from project_cloud.utils.utils_s3 import get_s3_object_keys, get_json_from_s3
load_dotenv()

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "departures"

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = "departures"


def create_stations_departures_dict(json_data,ingestion_timestamp_str):
    results = []
    departures = json_data["departures"]
    for index, departure in enumerate(departures):
        result = {
            "gid": index,
            "trip_id": departure.get("display_informations").get("trip_short_name"),
            "ingestion_timestamp": int(datetime.fromisoformat(ingestion_timestamp_str).timestamp() * 1_000_000),
            "departure_datetime": departure.get("stop_date_time").get("base_departure_date_time"),
            "departure_station": departure.get("stop_point").get("name"),
            "arrival_station": departure.get("display_informations").get("direction"),
            "network": departure.get("display_informations").get("network"),
            "type": "departures",
            "long": departure.get("stop_point",{}).get("coord").get("lon"),
            "lat": departure.get("stop_point", {}).get("coord").get("lat"),
        }
        results.append(result)
    return results

def main():
    last_processed_timestamp_str = get_last_processed_timestamp(dynamodb, DYNAMODB_TABLE_NAME,"departures")

    keys = get_s3_object_keys(s3, BUCKET_NAME, s3_folder)

    new_keys = []
    if last_processed_timestamp_str:
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
        print(key)
        response = get_json_from_s3(s3, BUCKET_NAME, key)
        ingestion_timestamp_str = key.split('/')[1]
        if response:
            results = create_stations_departures_dict(response,ingestion_timestamp_str)
            all_items_to_put.extend(results)

            if all_items_to_put:
                print(f"Attempting to batch put {len(all_items_to_put)} items to {DYNAMODB_TABLE_NAME}")
                batch_put_items_to_dynamodb(dynamodb, DYNAMODB_TABLE_NAME, all_items_to_put)
            else:
                print("No new data to process.")


if __name__ == "__main__":
    main()
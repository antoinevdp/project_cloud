import boto3
from dotenv import load_dotenv
import os
from datetime import datetime
from geopy.geocoders import Nominatim

from project_cloud.utils.utils_dynamodb import put_item_to_dynamodb, get_last_processed_timestamp, batch_put_items_to_dynamodb
from project_cloud.utils.utils_s3 import get_s3_object_keys, get_json_from_s3

load_dotenv()

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "parking"

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
DYNAMODB_TABLE_NAME = "parkings"

KEYS_TO_KEEP = [
    "gid", "nom", "gestionnaire", "id_gestionnaire", "insee", "adresse", "type_usagers",
    "gratuit", "nb_places", "nb_pr", "nb_pmr", "nb_voitures_electriques", "nb_velo",
    "nb_2r_el", "nb_autopartage", "nb_2_rm", "nb_covoit", "tarif_pmr", "tarif_1h",
    "tarif_2h", "tarif_3h", "tarif_4h", "tarif_24h", "abo_resident", "abo_non_resident",
    "type_ouvrage", "info", "places_disponibles", "etat", "last_update", "the_geom","ingestion_timestamp",
    "longitude", "latitude", "type"
]

def clean_json_data(data, keys_to_keep):
    cleaned_data = []
    for item in data:
        cleaned_item = {key: item.get(key) for key in keys_to_keep}
        cleaned_data.append(cleaned_item)
    return cleaned_data

def main():
    last_processed_timestamp_str = get_last_processed_timestamp(dynamodb, DYNAMODB_TABLE_NAME,"parkings")

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
            features = response.get("features",[])
            values_list = []
            for feature in features:
                value = feature.get("properties",{})
                long,lat = feature.get("geometry",{}).get("coordinates",[])
                value["longitude"] = long
                value["latitude"] = lat
                value["ingestion_timestamp"] = int(datetime.fromisoformat(ingestion_timestamp_str).timestamp() * 1_000_000)
                value["type"] = "parkings"
                values_list.append(value)
            cleaned_values = clean_json_data(values_list, KEYS_TO_KEEP)
            all_items_to_put.extend(cleaned_values)

            if all_items_to_put:
                print(f"Attempting to batch put {len(all_items_to_put)} items to {DYNAMODB_TABLE_NAME}")
                batch_put_items_to_dynamodb(dynamodb, DYNAMODB_TABLE_NAME, all_items_to_put)
            else:
                print("No new data to process.")

if __name__ == "__main__":
    main()
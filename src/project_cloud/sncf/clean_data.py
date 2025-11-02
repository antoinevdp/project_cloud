import boto3
import json
import os

from datetime import datetime
from project_cloud.utils.utils_s3 import get_json_from_s3

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"

def create_stations_departures_dict(json_data):
    results = []
    departures = json_data["departures"]
    for departure in departures:
        result = {
            "id": departure.get("display_informations").get("trip_short_name"),
            "ingestion_date": datetime.today().strftime('%Y-%m-%d'),
            "departure_datetime": departure.get("stop_date_time").get("base_departure_date_time"),
            "departure_station": departure.get("stop_point").get("name"),
            "arrival_station": departure.get("display_informations").get("direction")
        }
        results.append(result)
    return results


def create_dynamodb_table(table_name):
    dynamodb = boto3.client('dynamodb')
    try:
        dynamodb.create_table(
            TableName=table_name,
            KeySchema=[
                {
                    'AttributeName': 'id',
                    'KeyType': 'HASH'  # Partition key
                },
                {
                    'AttributeName': 'departure_station',
                    'KeyType': 'RANGE'  # Sort key
                }
            ],
            AttributeDefinitions=[
                {
                    'AttributeName': 'id',
                    'AttributeType': 'S'
                },
                {
                    'AttributeName': 'departure_station',
                    'AttributeType': 'S'
                }
            ],
            ProvisionedThroughput={
                'ReadCapacityUnits': 5,
                'WriteCapacityUnits': 5
            }
        )
        # Wait until the table exists.
        dynamodb.get_waiter('table_exists').wait(TableName=table_name)
        print(f"Table {table_name} created successfully.")
    except dynamodb.exceptions.ResourceInUseException:
        print(f"Table {table_name} already exists.")
        pass


def upload_to_dynamodb(data, table_name):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)


def main():
    table_name = "stations_departures"
    create_dynamodb_table(table_name)
    current_date = datetime.today().strftime('%Y%m%d')
    key = f"sncf/{current_date}/departures.json"
    json_content = get_json_from_s3(s3, BUCKET_NAME, key)
    if json_content:
        departures_data = create_stations_departures_dict(json_content)
        if departures_data:
            upload_to_dynamodb(departures_data, table_name)


if __name__ == "__main__":
    main()
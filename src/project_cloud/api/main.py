import boto3
from decimal import Decimal
import json

# --- Performance Improvement Note ---
# The functions below have been updated to use DynamoDB's query operation for better performance.
# This requires a Global Secondary Index (GSI) on the DynamoDB table.
#
# Recommended GSI for 'traffic' and 'parkings' tables:
# Index Name: 'gsi-type-ingestion_timestamp'
# Partition Key: 'type' (String) - This should be a new attribute with a constant value, e.g., 'traffic' or 'parking'.
# Sort Key: 'ingestion_timestamp' (Number)

def get_last_ingestion_time(dynamodb, table_name, type_value):
    table = dynamodb.Table(table_name)
    try:
        # Use the GSI to efficiently get the latest ingestion_timestamp
        response = table.query(
            IndexName='gsi-type-ingestion_timestamp',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('type').eq(type_value),
            ScanIndexForward=False,  # Sort by ingestion_timestamp in descending order
            Limit=1,
            ProjectionExpression="ingestion_timestamp"
        )
        items = response.get('Items', [])
        return items[0]['ingestion_timestamp'] if items else None
    except Exception as e:
        print(f"Error getting last ingestion time with query (GSI might be missing or not ready): {e}")
        # Fallback to the old scan method
        print("Falling back to scan operation for get_last_ingestion_time")
        response = table.scan(
            ProjectionExpression="ingestion_timestamp",
            Limit=1000  # Adjust as needed
        )
        timestamps = [item['ingestion_timestamp'] for item in response['Items']]
        return max(timestamps) if timestamps else None

def get_items_by_ingestion_time(dynamodb, table_name, ingestion_time, type_value):
    table = dynamodb.Table(table_name)
    try:
        response = table.query(
            IndexName='gsi-type-ingestion_timestamp',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('type').eq(type_value) & boto3.dynamodb.conditions.Key('ingestion_timestamp').eq(ingestion_time)
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting items by ingestion time with query (GSI might be missing or not ready): {e}")
        # Fallback to the old scan method
        print(f"Falling back to scan operation for get_{type_value}s_by_ingestion_time")
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('ingestion_timestamp').eq(ingestion_time)
        )
        return response.get('Items', [])

def decimal_serializer(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def lambda_handler(event, context):
    print(f"Received event: {event}")
    try:
        dynamodb = boto3.resource('dynamodb')

        path = event.get('path')
        print(f"Request path: {path}")

        if path == '/parkings':
            table_name = "parkings"
            type_value = "parkings"
        elif path == '/traffic':
            table_name = "traffic"
            type_value = "traffic"
        elif path == '/departures':
            table_name = "departures"
            type_value = "departures"
        else:
            print(f"Path not found: {path}")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': f'Not Found {path}'})
            }

        print(f"Table name: {table_name}, Type value: {type_value}")

        last_ingestion_time = get_last_ingestion_time(dynamodb, table_name, type_value)
        print(f"Last ingestion time: {last_ingestion_time}")

        if not last_ingestion_time:
            print("No ingestion time found.")
            return {
                'statusCode': 404,
                'body': json.dumps({'message': 'No ingestion time found'})
            }

        items = get_items_by_ingestion_time(dynamodb, table_name, last_ingestion_time, type_value)
        print(f"Retrieved {len(items)} items.")

        # Sort items by 'gid' if 'gid' exists in the items
        if items and any('gid' in item for item in items):
            print("Sorting items by gid.")
            items.sort(key=lambda item: item.get('gid', float('inf')))

        print("Returning successful response.")
        return {
            'statusCode': 200,
            'body': json.dumps(items, default=decimal_serializer)
        }
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': 'Internal Server Error',
                'error': e
            })
        }

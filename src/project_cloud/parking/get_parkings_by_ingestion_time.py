import boto3
from decimal import Decimal
import json

def get_last_ingestion_time(dynamodb, table_name):
    table = dynamodb.Table(table_name)
    try:
        # This is inefficient, a better way would be to have a GSI on ingestion_timestamp
        response = table.scan(
            ProjectionExpression="ingestion_timestamp",
            Limit=1000 # Adjust as needed
        )
        timestamps = [item['ingestion_timestamp'] for item in response['Items']]
        return max(timestamps) if timestamps else None
    except Exception as e:
        print(f"Error getting last ingestion time: {e}")
        return None

def get_parkings_by_ingestion_time(dynamodb, table_name, ingestion_time):
    table = dynamodb.Table(table_name)
    try:
        response = table.scan(
            FilterExpression=boto3.dynamodb.conditions.Attr('ingestion_timestamp').eq(ingestion_time)
        )
        return response.get('Items', [])
    except Exception as e:
        print(f"Error getting parkings by ingestion time: {e}")
        return []

def lambda_handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table_name = "parkings"

    last_ingestion_time = get_last_ingestion_time(dynamodb, table_name)

    if not last_ingestion_time:
        return {
            'statusCode': 404,
            'body': json.dumps({'message': 'No ingestion time found'})
        }

    items = get_parkings_by_ingestion_time(dynamodb, table_name, last_ingestion_time)

    # Convert Decimal to float for JSON serialization
    for item in items:
        for key, value in item.items():
            if isinstance(value, Decimal):
                item[key] = float(value)

    return {
        'statusCode': 200,
        'body': json.dumps(items)
    }

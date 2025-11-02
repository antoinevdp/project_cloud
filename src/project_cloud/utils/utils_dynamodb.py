from decimal import Decimal
import boto3

def put_item_to_dynamodb(dynamodb, table_name, item):
    table = dynamodb.Table(table_name)
    try:
        item_decimal = replace_floats(item)
        table.put_item(Item=item_decimal)
        print(f"Successfully put item {item.get('gid')} to {table_name}")
    except Exception as e:
        print(f"Error putting item to DynamoDB: {e}")

def replace_floats(obj):
    if isinstance(obj, list):
        return [replace_floats(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: replace_floats(v) for k, v in obj.items()}
    elif isinstance(obj, float):
        return Decimal(str(obj))
    else:
        return obj

def get_last_processed_timestamp(dynamodb, table_name):
    table = dynamodb.Table(table_name)
    try:
        response = table.query(
            IndexName='gsi-type-ingestion_timestamp',
            KeyConditionExpression=boto3.dynamodb.conditions.Key('type').eq('traffic'), # Replace 'traffic' if your type is different
            ScanIndexForward=False,  # Sort by ingestion_timestamp in descending order
            Limit=1,
            ProjectionExpression="ingestion_timestamp"
        )
        items = response.get('Items', [])
        return items[0]['ingestion_timestamp'] if items else None
    except Exception as e:
        print(f"Error getting last processed timestamp with GSI: {e}")
        # Fallback to scan if GSI is not ready or does not exist
        print("Falling back to scan operation.")
        try:
            response = table.scan(
                ProjectionExpression="ingestion_timestamp",
                Limit=1000 # Adjust as needed
            )
            timestamps = [item['ingestion_timestamp'] for item in response['Items']]
            return max(timestamps) if timestamps else None
        except Exception as scan_e:
            print(f"Error during fallback scan: {scan_e}")
            return None

def batch_put_items_to_dynamodb(dynamodb, table_name, items):
    table = dynamodb.Table(table_name)
    try:
        with table.batch_writer() as batch:
            for item in items:
                item_decimal = replace_floats(item)
                batch.put_item(Item=item_decimal)
        print(f"Successfully batch put {len(items)} items to {table_name}")
    except Exception as e:
        print(f"Error batch putting items to DynamoDB: {e}")
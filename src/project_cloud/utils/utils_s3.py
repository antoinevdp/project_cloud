import json
from datetime import datetime

def upload_to_s3(s3, json_data, bucket_name, key):
    try:
        s3.put_object(Bucket=bucket_name, Key=key, Body=json.dumps(json_data, indent=4))
        print(f"Uploaded {key} to {bucket_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def get_json_from_s3(s3, bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print(f"Retrieved {key} from {bucket}")
        content = response["Body"].read().decode("utf-8")
        return json.loads(content)
    except s3.exceptions.NoSuchKey:
        print(f"Error: Object with key '{key}' not found in bucket '{bucket}'.")
        return None
    except json.JSONDecodeError:
        print(f"Error: Failed to decode JSON from object with key '{key}' in bucket '{bucket}'.")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def get_existing_timestamps(s3, bucket_name, prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        timestamps = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                timestamp = key.split('/')[1]
                timestamps.append(timestamp)
        return timestamps
    except Exception as e:
        print(f"Error listing objects from S3: {e}")
        return []

def get_s3_object_keys(s3, bucket_name, prefix):
    try:
        response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        keys = []
        if 'Contents' in response:
            for obj in response['Contents']:
                keys.append(obj['Key'])
        return keys
    except Exception as e:
        print(f"Error listing objects from S3: {e}")
        return []
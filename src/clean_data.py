import boto3
import json
import os

from datetime import datetime

# S3 setup
s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"


def get_json_from_s3(bucket, key):
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


def main():
    current_date = datetime.today().strftime('%Y%m%d')
    key = f"sncf/{current_date}/departures.json"
    json_content = get_json_from_s3(BUCKET_NAME, key)
    if json_content:
        print(json.dumps(json_content, indent=4))


if __name__ == "__main__":
    main()
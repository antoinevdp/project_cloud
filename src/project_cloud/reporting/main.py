import boto3
from datetime import datetime
from project_cloud.utils.utils_dynamodb import get_all_items_by_date
from project_cloud.utils.utils_s3 import get_s3_object_keys, get_json_from_s3


s3 = boto3.client("s3")
BUCKET_NAME = "efrei-cloud-project"
s3_folder = "parking"

# DynamoDB setup
dynamodb = boto3.resource('dynamodb')
reporting_tables = ["aggregation_average_availability_parking", "aggregation_number_of_parkings_in_operation",
                    "aggregation_overall_occupancy_rate", "aggregation_reference_pricing"]

def main():
    for table in reporting_tables:
        curent_date = datetime.today().strftime('%Y-%m-%d')
        items = get_all_items_by_date(dynamodb, table, curent_date)
        print(items)
        return

if __name__ == "__main__":
    main()
#!/bin/bash

echo "Starting the pipeline..."

echo "--- Ingesting Parking Data ---"
python -m project_cloud.parking.main

echo "--- Processing Parking Data ---"
python -m project_cloud.parking.process_s3_to_dynamodb

echo "--- Ingesting Traffic Data ---"
python -m project_cloud.traffic.main

echo "--- Processing Traffic Data ---"
python -m project_cloud.traffic.process_s3_to_dynamodb

echo "Pipeline finished."

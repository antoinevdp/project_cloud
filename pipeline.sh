cd $HOME/project_cloud/
#!/bin/bash

echo "Starting the pipeline..."
/home/ubuntu/.local/bin/uv sync --locked
echo "--- Ingesting Parking Data ---"
/home/ubuntu/.local/bin/uv run ingest-parking

echo "--- Processing Parking Data ---"
/home/ubuntu/.local/bin/uv run process-parking

echo "--- Ingesting Traffic Data ---"
/home/ubuntu/.local/bin/uv run ingest-traffic

echo "--- Processing Traffic Data ---"
/home/ubuntu/.local/bin/uv run process-traffic

echo "Pipeline finished."
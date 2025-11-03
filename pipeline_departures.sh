cd $HOME/project_cloud/
#!/bin/bash

echo "Starting the pipeline..."
/home/ubuntu/.local/bin/uv sync --locked

echo "--- Ingesting Departures Data ---"
/home/ubuntu/.local/bin/uv run ingest-departures
sleep 1

echo "--- Processing Departures Data ---"
/home/ubuntu/.local/bin/uv run process-departures

echo "Pipeline finished."
cd $HOME/project_cloud/
#!/bin/bash

echo "Starting the pipeline..."
/home/ubuntu/.local/bin/uv sync --locked

echo "--- Creating reports ---"
/home/ubuntu/.local/bin/uv run create-reports

echo "Pipeline finished."
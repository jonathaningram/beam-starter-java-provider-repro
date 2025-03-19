#!/usr/bin/env bash

set -euo pipefail

if [ $# -lt 1 ]; then
    echo "Usage: $0 <pipeline_yaml_file>"
    exit 1
fi

PIPELINE_FILE="$1"
shift

if [ ! -f "$PIPELINE_FILE" ]; then
    echo "Error: Pipeline file '$PIPELINE_FILE' not found"
    exit 1
fi

docker build -t beam_python3.11_sdk_with_java:2.63.0 .

docker run -v "$(pwd):/app" \
    -v ~/.config/gcloud:/root/.config/gcloud \
    -w /app \
    --entrypoint /bin/bash beam_python3.11_sdk_with_java:2.63.0 \
    -c "python -m apache_beam.yaml.main --yaml_pipeline='$(yq -o=json '.' "$PIPELINE_FILE")' --runner=DataflowRunner"

#!/usr/bin/env bash

set -euo pipefail

if [ $# -lt 2 ]; then
    echo "Usage: $0 <network_name> <job_name> <pipeline_yaml_file>"
    exit 1
fi

NETWORK_NAME="$1"
JOB_NAME="$2"
PIPELINE_FILE="$3"

if [ ! -f "$PIPELINE_FILE" ]; then
    echo "Error: Pipeline file '$PIPELINE_FILE' not found"
    exit 1
fi

gcloud dataflow yaml run "$JOB_NAME" \
  --yaml-pipeline-file="$PIPELINE_FILE" \
  --region="us-central1" \
  --pipeline-options="network=$NETWORK_NAME"

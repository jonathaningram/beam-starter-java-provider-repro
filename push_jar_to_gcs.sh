#!/usr/bin/env bash

LOCAL_FILE="target/xlang-transforms-bundled-1.0-SNAPSHOT.jar"
BUCKET="gs://beam-starter-java-provider-repro"
FILENAME=$(basename "$LOCAL_FILE")  # Extract filename
EXTENSION="${FILENAME##*.}"         # Extract extension
BASENAME="${FILENAME%.*}"           # Extract basename

NEW_FILENAME="${BASENAME}.${EXTENSION}"

DEST_PATH="${BUCKET}/beam-2.63.0/${NEW_FILENAME}"

gcloud storage cp "$LOCAL_FILE" "$DEST_PATH"

echo "$DEST_PATH"

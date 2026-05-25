#!/bin/bash

set -e

BUCKET_NAME="pokemon-glue-project-akemi"

LOCAL_SCRIPT_PATH="./jobs/main.py"

S3_SCRIPT_PATH="s3://${BUCKET_NAME}/scripts/main.py"

echo "Enviando script Glue para:"
echo "$S3_SCRIPT_PATH"

aws s3 cp "$LOCAL_SCRIPT_PATH" "$S3_SCRIPT_PATH"

echo "Upload concluído com sucesso."
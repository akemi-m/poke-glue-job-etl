#!/bin/bash

# =========================================================
# Script para enviar o JSON local para o S3 particionado por data
# =========================================================

BUCKET_NAME="pokemon-glue-project-akemi"

YEAR=$(date +%Y)
MONTH=$(date +%m)
DAY=$(date +%d)

LOCAL_FILE_PATH="./sample-data/pokemons.json"
S3_DESTINATION_PATH="s3://${BUCKET_NAME}/raw/pokemons/year=${YEAR}/month=${MONTH}/day=${DAY}/pokemons.json"

aws s3 cp "$LOCAL_FILE_PATH" "$S3_DESTINATION_PATH"

echo "Upload concluído:"
echo "$S3_DESTINATION_PATH"
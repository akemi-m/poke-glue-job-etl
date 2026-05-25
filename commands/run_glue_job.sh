#!/bin/bash

set -e

JOB_NAME="pokemon-glue-etl-job"

echo "Executando Glue Job: $JOB_NAME"

aws glue start-job-run \
    --job-name "$JOB_NAME"

echo "Execução iniciada com sucesso."
#!/usr/bin/env bash
# Source this file (do not execute):  source scripts/airflow_env.sh
_PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

source "${VENV_DIR:-$HOME/venvs/mapping-airflow}/bin/activate"

# Airflow metadata + the mapping DB live on the Linux filesystem: SQLite locking on /mnt/c (OneDrive) is unreliable and slow.
export AIRFLOW_HOME="${AIRFLOW_HOME:-$HOME/airflow_home}"
export AIRFLOW__CORE__DAGS_FOLDER="${_PROJECT_DIR}/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

export INPUT_DIR="${INPUT_DIR:-${_PROJECT_DIR}/data/input}"          # read-only inputs may stay on /mnt/c
export REFERENCE_FILE="${REFERENCE_FILE:-${_PROJECT_DIR}/data/reference/canonical_customers.csv}"
export OUTPUT_DIR="${OUTPUT_DIR:-$HOME/mapping_data/output}"
export MAPPING_DB_PATH="${MAPPING_DB_PATH:-$HOME/mapping_data/mapping.db}"
mkdir -p "${OUTPUT_DIR}" "$(dirname "${MAPPING_DB_PATH}")"

echo "AIRFLOW_HOME=${AIRFLOW_HOME}  DB=${MAPPING_DB_PATH}  OUTPUT=${OUTPUT_DIR}"

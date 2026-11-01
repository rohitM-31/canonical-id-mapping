#!/usr/bin/env bash
# One-time setup of Airflow (no Docker) inside WSL (Ubuntu). Re-runnable.
#   bash scripts/setup_airflow_wsl.sh
# Airflow 2.10.5 supports Python 3.8-3.12. If the system python3 is newer (e.g. 3.14), a private
# Python 3.12 is installed with `uv` (no sudo, system Python untouched).
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_DIR="${VENV_DIR:-$HOME/venvs/mapping-airflow}"
AIRFLOW_VERSION="2.10.5"
TARGET_PY="3.12"

export PATH="$HOME/.local/bin:$PATH"
if ! command -v uv >/dev/null 2>&1; then
  echo "installing uv (python manager)"
  if command -v curl >/dev/null 2>&1; then curl -LsSf https://astral.sh/uv/install.sh | sh
  else wget -qO- https://astral.sh/uv/install.sh | sh; fi
  export PATH="$HOME/.local/bin:$PATH"
fi

rm -rf "${VENV_DIR}"
uv venv --python "${TARGET_PY}" "${VENV_DIR}"
PYBIN="${VENV_DIR}/bin/python"
echo "venv python = $("${PYBIN}" --version)"

uv pip install --python "${PYBIN}" "apache-airflow==${AIRFLOW_VERSION}" \
  --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${TARGET_PY}.txt"
uv pip install --python "${PYBIN}" -r "${PROJECT_DIR}/requirements.txt"

# shellcheck disable=SC1091
source "${PROJECT_DIR}/scripts/airflow_env.sh"
airflow db migrate
echo
echo "Setup done. Next:"
echo "  source scripts/airflow_env.sh"
echo "  airflow dags test source_mapping        # run the whole DAG once, no scheduler needed"
echo "  airflow standalone                      # UI on http://localhost:8080 (credentials printed on start-up)"

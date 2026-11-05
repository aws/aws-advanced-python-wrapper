#!/usr/bin/env bash
# The compose lane of the KMS encryption tests: PostgreSQL, MySQL and LocalStack's KMS on 127.0.0.1, and the KMS
# container modules run on this host with a hand-written TEST_ENV_INFO_JSON, without the Gradle harness.
#
# Usage: run.sh up | test <PG|MYSQL> [pytest arguments] | real <PG|MYSQL> [pytest arguments] | down
#
# Export DB_USER, DB_NAME and a throwaway DB_PASSWORD; optionally PG_PORT, MYSQL_PORT and KMS_PORT (59210 to 59212),
# COMPOSE ("docker compose", the default, or "finch compose") and COMPOSE_PROJECT_NAME (kms-local).
# finch compose doesn't read exported variables, so each compose command gets them in an env file under $HOME that is
# deleted when the command ends. Finch hands that file to every container, LocalStack included.
# real runs the real-key module against KMS_KEY_ARN in KMS_REGION, with the caller's AWS credentials (for example
# AWS_PROFILE) and KMS_DECRYPT_ONLY_ROLE_ARN; KMS_MANUAL_TESTS=1 adds its manual tests.
# The JUnit report goes to tests/integration/container/reports/kms-local-<engine>.xml (kms-real-<engine>.xml for real),
# and up's output to kms-local-up.log there (mode 600).
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
root="$(cd "${here}/../../.." && pwd)"
: "${DB_USER:?export DB_USER}" "${DB_PASSWORD:?export DB_PASSWORD}" "${DB_NAME:?export DB_NAME}"
export PG_PORT="${PG_PORT:-59210}" MYSQL_PORT="${MYSQL_PORT:-59211}" KMS_PORT="${KMS_PORT:-59212}"
read -r -a compose <<< "${COMPOSE:-docker compose}"
project="${COMPOSE_PROJECT_NAME:-kms-local}"
env_file=""
trap 'rm -f "${env_file}"' EXIT

run_compose() {
  local rc=0
  env_file="$(mktemp "${HOME}/.kms-local-env.XXXXXX")"
  chmod 600 "${env_file}"
  printf '%s=%s\n' DB_USER "${DB_USER}" DB_PASSWORD "${DB_PASSWORD}" DB_NAME "${DB_NAME}" \
    PG_PORT "${PG_PORT}" MYSQL_PORT "${MYSQL_PORT}" KMS_PORT "${KMS_PORT}" > "${env_file}"
  (cd "${HOME}" && "${compose[@]}" -p "${project}" -f "${here}/compose.yaml" --env-file "${env_file}" "$@") || rc=$?
  rm -f "${env_file}"
  return "${rc}"
}

# finch compose ignores healthcheck:, so each service is probed from this host until it answers, for up to 240 s.
wait_for_services() {
  (cd "${root}" && poetry run python - <<'PY'
import json
import os
import time
import urllib.request

import mysql.connector
import psycopg

e = os.environ


def postgres_answers():
    psycopg.connect(host="127.0.0.1", port=int(e["PG_PORT"]), user=e["DB_USER"], password=e["DB_PASSWORD"],
                    dbname=e["DB_NAME"], connect_timeout=3).close()


def mysql_answers():
    mysql.connector.connect(host="127.0.0.1", port=int(e["MYSQL_PORT"]), user="root", password=e["DB_PASSWORD"],
                            database=e["DB_NAME"], connection_timeout=3).close()


def kms_answers():
    with urllib.request.urlopen(f"http://127.0.0.1:{e['KMS_PORT']}/_localstack/health", timeout=3) as response:
        if json.load(response)["services"]["kms"] not in ("available", "running"):
            raise RuntimeError("kms isn't available yet")


pending = {"postgres": postgres_answers, "mysql": mysql_answers, "localstack": kms_answers}
deadline = time.monotonic() + 240
while pending and time.monotonic() < deadline:
    for name, probe in list(pending.items()):
        try:
            probe()
        except Exception:
            continue
        print(f"{name} is ready", flush=True)
        del pending[name]
    if pending:
        time.sleep(2)
if pending:
    raise SystemExit(f"Not ready after 240 s: {', '.join(sorted(pending))}")
PY
  )
}

case "${1:-}" in
  up)
    # Compose may print each container's environment, the password included, so up's output goes to a mode-600 file.
    up_log="${root}/tests/integration/container/reports/kms-local-up.log"
    mkdir -p "$(dirname "${up_log}")"
    : > "${up_log}"
    chmod 600 "${up_log}"
    if ! run_compose up -d > "${up_log}" 2>&1; then
      echo "compose up failed; its output is in ${up_log}" >&2
      exit 1
    fi
    echo "compose up done; its output is in ${up_log}"
    wait_for_services
    ;;
  test | real)
    mode="$1"
    engine="${2:-}"
    if [[ "${engine}" != PG && "${engine}" != MYSQL ]]; then
      echo "Usage: run.sh ${mode} <PG|MYSQL> [pytest arguments]" >&2
      exit 2
    fi
    shift 2
    cd "${root}"
    # By path, never with -k kms, which also matches folder names.
    modules=()
    for module in tests/integration/container/test_kms_encryption_*.py \
        tests/integration/container/sqlalchemy/test_sqlalchemy_kms_encryption.py; do
      if [[ -f "${module}" ]]; then
        modules+=("${module}")
      fi
    done
    if [[ "${engine}" == MYSQL && -f tests/integration/container/django/test_django_kms_encryption.py ]]; then
      modules+=(tests/integration/container/django/test_django_kms_encryption.py)
    fi
    if [[ ${#modules[@]} -eq 0 ]]; then
      echo "No KMS encryption test modules under tests/integration/container." >&2
      exit 2
    fi
    export ENGINE="${engine}" FEATURE="KMS_LOCALSTACK"
    if [[ "${mode}" == real ]]; then
      : "${KMS_KEY_ARN:?export KMS_KEY_ARN}" "${KMS_REGION:?export KMS_REGION}"
      modules=(tests/integration/container/test_kms_encryption_real_kms.py)
      FEATURE="AWS_CREDENTIALS_ENABLED"
    fi
    # rdsEndpoint is LocalStack's port in both modes, so conftest's RDS prewarm never reaches AWS.
    TEST_ENV_INFO_JSON="$(python3 -c 'import json, os; e = os.environ; port = int(e["PG_PORT"] if e["ENGINE"] == "PG" else e["MYSQL_PORT"]); print(json.dumps({"request": {"engine": e["ENGINE"], "instances": "SINGLE_INSTANCE", "deployment": "DOCKER", "targetPythonVersion": "PYTHON_3_12", "numOfInstances": 1, "features": ["RUN_KMS_ENCRYPTION_TESTS_ONLY", e["FEATURE"]]}, "region": e.get("KMS_REGION", "us-east-1"), "rdsEndpoint": "http://127.0.0.1:" + e["KMS_PORT"], "databaseInfo": {"username": e["DB_USER"], "password": e["DB_PASSWORD"], "defaultDbName": e["DB_NAME"], "instances": [{"instanceId": e["ENGINE"].lower(), "host": "127.0.0.1", "port": port}]}}))')"
    export TEST_ENV_INFO_JSON
    if [[ "${mode}" == real ]]; then
      env -u KMS_ENDPOINT_URL AWS_REGION="${KMS_REGION}" \
        poetry run python -m pytest -p no:cacheprovider -p no:logging \
        --junitxml="tests/integration/container/reports/kms-real-${engine}.xml" "${modules[@]}" "$@"
      exit
    fi
    # The harness's pytest options that change what a test sees: -p no:logging, so no test can rely on caplog.
    env -u AWS_SESSION_TOKEN -u AWS_SECURITY_TOKEN -u AWS_PROFILE -u AWS_DEFAULT_PROFILE \
      AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1 \
      AWS_CONFIG_FILE=/dev/null AWS_SHARED_CREDENTIALS_FILE=/dev/null AWS_EC2_METADATA_DISABLED=true \
      KMS_ENDPOINT_URL="http://127.0.0.1:${KMS_PORT}" KMS_REGION=us-east-1 \
      poetry run python -m pytest -p no:cacheprovider -p no:logging \
      --junitxml="tests/integration/container/reports/kms-local-${engine}.xml" "${modules[@]}" "$@"
    ;;
  down)
    run_compose down -v
    ;;
  *)
    echo "Usage: run.sh up | test <PG|MYSQL> [pytest arguments] | real <PG|MYSQL> [pytest arguments] | down" >&2
    exit 2
    ;;
esac

#!/bin/bash

CURRENT_WORKDIR=$PWD
COMPUTATION_ENGINE="../computation_engine"
ENV_FILE="${CURRENT_WORKDIR}/.env"
DOCKER_COMPOSE_FILE="${COMPUTATION_ENGINE}/docker-compose.yml"
DOCKER_COMPOSE_OVERRIDE_FILE="./docker-compose.override.yml"

cp ${COMPUTATION_ENGINE}/.env-template ${ENV_FILE}

${COMPUTATION_ENGINE}/scripts/setup_orchestrator_postgres_db.sh ${ENV_FILE} ${DOCKER_COMPOSE_FILE}
${COMPUTATION_ENGINE}/scripts/setup_rabbitmq.sh ${ENV_FILE} ${DOCKER_COMPOSE_FILE}
echo "Using docker compose files: ${DOCKER_COMPOSE_FILE} ${DOCKER_COMPOSE_OVERRIDE_FILE}"

export ORCHESTRATOR_DIR="${CURRENT_WORKDIR}/../"
export TEST_WORKER_DIR="${CURRENT_WORKDIR}/test_worker/"

docker compose -f ${DOCKER_COMPOSE_FILE} -f ${DOCKER_COMPOSE_OVERRIDE_FILE} --env-file ${ENV_FILE} up --build -d orchestrator test_worker

python3 -m venv ./.venv/
. .venv/bin/activate
pip install -r requirements.txt

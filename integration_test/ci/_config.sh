#!/bin/bash

CURRENT_WORKDIR=$PWD
COMPUTATION_ENGINE="../computation-engine-at-orchestrator"
ENV_FILE="${CURRENT_WORKDIR}/.env.test"
DOCKER_COMPOSE_FILE="${COMPUTATION_ENGINE}/docker-compose.yml"
DOCKER_COMPOSE_OVERRIDE_FILE="./docker-compose.override.yml"

export COMPOSE_PROJECT_NAME=omotes_orchestrator_integration_tests

export ORCHESTRATOR_DIR="${CURRENT_WORKDIR}/../"
export TEST_WORKER_DIR="${CURRENT_WORKDIR}/test_worker/"
export INTEGRATION_TESTS_DIR="${CURRENT_WORKDIR}/integration_tests/"

echo "Using docker compose files: ${DOCKER_COMPOSE_FILE} ${DOCKER_COMPOSE_OVERRIDE_FILE}"

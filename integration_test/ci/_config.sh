#!/bin/bash

export ORCHESTRATOR_TEST_DIR=$PWD
export OMOTES_SYSTEM="../omotes-system-at-orchestrator"
export ORCHESTRATOR_DIR="${ORCHESTRATOR_TEST_DIR}/../"
export TEST_WORKER_DIR="${ORCHESTRATOR_TEST_DIR}/test_worker/"
export INTEGRATION_TESTS_DIR="${ORCHESTRATOR_TEST_DIR}/integration_tests/"
export ENV_FILE="${ORCHESTRATOR_TEST_DIR}/.env.test"

export DOCKER_COMPOSE_SETUP_TEST_OVERRIDE="${OMOTES_SYSTEM}/docker-compose.yml -f ${OMOTES_SYSTEM}/docker-compose.override.setup.yml -f ${ORCHESTRATOR_TEST_DIR}/docker-compose.override.yml"
export DOCKER_COMPOSE_TEST_OVERRIDE="${OMOTES_SYSTEM}/docker-compose.yml -f ${ORCHESTRATOR_TEST_DIR}/docker-compose.override.yml"

export COMPOSE_PROJECT_NAME=omotes_orchestrator_integration_tests

echo "Using docker compose files for setup: ${DOCKER_COMPOSE_SETUP_TEST_OVERRIDE}"
echo "Using docker compose files for test: ${DOCKER_COMPOSE_TEST_OVERRIDE}"

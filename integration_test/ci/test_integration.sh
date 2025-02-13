#!/bin/bash

. ci/_setup.sh

echo "Starting integration test."
timeout -s=2 120 docker compose -f ${DOCKER_COMPOSE_TEST_OVERRIDE} --env-file ${ENV_FILE} up --build --attach-dependencies --abort-on-container-exit integration_tests

#!/bin/bash

. ci/_setup.sh

echo "Starting integration test."
docker compose -f ${DOCKER_COMPOSE_TEST_OVERRIDE} --env-file ${ENV_FILE} up --build --attach-dependencies --abort-on-container-exit integration_tests

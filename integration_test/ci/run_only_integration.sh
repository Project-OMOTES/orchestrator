#!/bin/bash

. ci/_config.sh

docker compose -f ${DOCKER_COMPOSE_TEST_OVERRIDE} --env-file ${ENV_FILE} up --build integration_tests

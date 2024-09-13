#!/bin/bash

. ci/_setup.sh

docker compose -f ${DOCKER_COMPOSE_FILE} -f ${DOCKER_COMPOSE_OVERRIDE_FILE} --env-file ${ENV_FILE} up --build --attach-dependencies --abort-on-container-exit integration_tests

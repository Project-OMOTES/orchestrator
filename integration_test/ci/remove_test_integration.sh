#!/bin/bash

. ci/_config.sh

docker compose -f ${DOCKER_COMPOSE_FILE} -f ${DOCKER_COMPOSE_OVERRIDE_FILE} --env-file ${ENV_FILE} down -v

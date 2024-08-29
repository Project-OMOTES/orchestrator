#!/bin/bash

. ci/_setup.sh

docker compose -f ${DOCKER_COMPOSE_FILE} -f ${DOCKER_COMPOSE_OVERRIDE_FILE} --env-file ${ENV_FILE} up --build rabbitmq omotes_influxdb orchestrator test_worker test_hard_crash_worker test_long_sleep_worker

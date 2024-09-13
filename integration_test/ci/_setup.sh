#!/bin/bash

. ci/_config.sh

cp ${COMPUTATION_ENGINE}/.env.template ${ENV_FILE}
sed -i 's/LOG_LEVEL=[a-z]*/LOG_LEVEL=WARNING  /gi' ${ENV_FILE}

docker compose -f ${DOCKER_COMPOSE_FILE} -f ${DOCKER_COMPOSE_OVERRIDE_FILE} --env-file ${ENV_FILE} down -v

${COMPUTATION_ENGINE}/scripts/setup_orchestrator_postgres_db.sh ${ENV_FILE} ${DOCKER_COMPOSE_FILE}
${COMPUTATION_ENGINE}/scripts/setup_rabbitmq.sh ${ENV_FILE} ${DOCKER_COMPOSE_FILE}

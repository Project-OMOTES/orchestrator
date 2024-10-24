#!/bin/bash

. ci/_config.sh

cp ${OMOTES_SYSTEM}/.env.template ${ENV_FILE}
sed -i 's/LOG_LEVEL=[a-z]*/LOG_LEVEL=WARNING  /gi' ${ENV_FILE}

docker compose -f ${DOCKER_COMPOSE_TEST_OVERRIDE} --env-file ${ENV_FILE} down -v

echo "Setting up OMOTES system"
${OMOTES_SYSTEM}/scripts/setup.sh "${ENV_FILE}" "${DOCKER_COMPOSE_SETUP_TEST_OVERRIDE}"
echo "Setup for OMOTES system finished."

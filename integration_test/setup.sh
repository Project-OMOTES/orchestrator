#!/bin/bash

. .env

DOCKER_COMPOSE="docker compose"

# Deploy postgres omotes schema
$DOCKER_COMPOSE up -d --wait orchestrator_postgres_db
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d postgres -c 'CREATE DATABASE omotes_jobs;'
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "CREATE USER ${POSTGRES_ORCHESTRATOR_USER_NAME} WITH PASSWORD '${POSTGRES_ORCHESTRATOR_USER_PASSWORD}';"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "ALTER USER ${POSTGRES_ORCHESTRATOR_USER_NAME} WITH PASSWORD '${POSTGRES_ORCHESTRATOR_USER_PASSWORD}';"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "GRANT ALL PRIVILEGES ON DATABASE omotes_jobs TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "GRANT ALL PRIVILEGES ON SCHEMA public TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON TABLES TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"
$DOCKER_COMPOSE exec orchestrator_postgres_db psql -d omotes_jobs -c "ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL ON SEQUENCES TO ${POSTGRES_ORCHESTRATOR_USER_NAME};"

# Upgrade omotes tables
$DOCKER_COMPOSE build orchestrator_postgres_db_upgrade
$DOCKER_COMPOSE run --rm orchestrator_postgres_db_upgrade

# Setup rabbitmq
$DOCKER_COMPOSE up -d --wait rabbitmq
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl add_vhost omotes
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl set_permissions --vhost omotes root ".*" ".*" ".*"
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl add_vhost omotes_celery
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl set_permissions --vhost omotes_celery root ".*" ".*" ".*"

$DOCKER_COMPOSE exec rabbitmq rabbitmqctl add_user --vhost omotes "${RABBITMQ_OMOTES_USER_NAME}" "${RABBITMQ_OMOTES_USER_PASSWORD}"
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl set_permissions --vhost omotes "${RABBITMQ_OMOTES_USER_NAME}" ".*" ".*" ".*"
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl add_user --vhost omotes_celery "${RABBITMQ_CELERY_USER_NAME}" "${RABBITMQ_CELERY_USER_PASSWORD}"
$DOCKER_COMPOSE exec rabbitmq rabbitmqctl set_permissions --vhost omotes_celery "${RABBITMQ_CELERY_USER_NAME}" ".*" ".*" ".*"

# Scaled integration test
Submits a large number of jobs from multiples processes/SDKs to check if all of the jobs 
succeed successfully.

# Setup and run the test
First, ensure that the `computation_engine` repository is available at the same level as
`orchestrator`.

1. Make sure `.env` is available with all env vars:
```bash
POSTGRES_ROOT_USER=root
POSTGRES_ROOT_PASSWORD=1234
POSTGRES_DEV_PORT=6432
POSTGRES_ORCHESTRATOR_USER_NAME=omotes_orchestrator
POSTGRES_ORCHESTRATOR_USER_PASSWORD=somepass3

RABBITMQ_ROOT_USER=root
RABBITMQ_ROOT_PASSWORD=5678
RABBITMQ_HIPE_COMPILE=1
RABBITMQ_EXCHANGE=nwn
RABBITMQ_OMOTES_USER_NAME=omotes
RABBITMQ_OMOTES_USER_PASSWORD=somepass1
RABBITMQ_CELERY_USER_NAME=celery
RABBITMQ_CELERY_USER_PASSWORD=somepass2
```

2. Run `./setup.sh`
3. Run `docker compose up --build`
4. To setup the environment to submit all the jobs:
    - `python3.11 -m venv ./.venv/`
    - `pip install -r ./requirements.txt`
4. Run `run.sh` to start the test.

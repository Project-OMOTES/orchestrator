FROM python:3.11-slim-buster

WORKDIR /app


RUN apt update && \
    apt install -y libpq-dev python3-dev gcc make  # Install dependencies needed for psycopg2

COPY requirements.txt /app/omotes_orchestrator/requirements.txt
RUN pip install -r /app/omotes_orchestrator/requirements.txt --no-cache-dir

COPY src/omotes_orchestrator    /app/omotes_orchestrator/

ENV PYTHONPATH="/app/"

CMD ["python", "-m", "omotes_orchestrator.main"]

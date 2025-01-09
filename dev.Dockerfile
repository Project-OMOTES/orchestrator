FROM python:3.13-slim-buster

WORKDIR /app


RUN apt update && \
    apt install -y libpq-dev python3-dev gcc make  # Install dependencies needed for psycopg2

COPY orchestrator/requirements.txt /app/requirements.txt
RUN pip install -r /app/requirements.txt --no-cache-dir

COPY ../omotes-sdk-protocol/python/ /omotes-sdk-protocol/python/
COPY ../omotes-sdk-python/ /omotes-sdk-python/
RUN pip install /omotes-sdk-python/
RUN pip install /omotes-sdk-protocol/python/

COPY orchestrator/src/ /app/

ENV PYTHONPATH="/app/"

CMD ["/app/start.sh"]

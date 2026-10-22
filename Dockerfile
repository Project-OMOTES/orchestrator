FROM python:3.11-slim-bookworm

WORKDIR /app


RUN apt update && \
    apt install -y --only-upgrade ca-certificates && \
    apt install -y libpq-dev python3-dev gcc make  # Install dependencies needed for psycopg2

COPY requirements.txt /app/requirements.txt
RUN pip install --upgrade 'pip>=26.2.2' && \
    pip install -r /app/requirements.txt --no-cache-dir

COPY src/ /app/

ENV PYTHONPATH="/app/"

CMD ["/app/start.sh"]

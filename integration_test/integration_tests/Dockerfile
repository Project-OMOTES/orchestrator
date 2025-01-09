FROM python:3.13-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r    /app/requirements.txt --no-cache-dir

COPY src/   /app/

CMD ["pytest"]

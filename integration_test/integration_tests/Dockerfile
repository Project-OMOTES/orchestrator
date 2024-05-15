FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install -r    /app/requirements.txt --no-cache-dir

COPY job_submitter.py   /app/

CMD ["python3", "job_submitter.py"]

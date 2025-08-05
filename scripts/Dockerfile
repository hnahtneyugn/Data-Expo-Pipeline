FROM python:3.12.11-alpine

RUN apt-get install wget
RUN pip install --no-cache-dir google-cloud-storage

WORKDIR /app

COPY scripts/ingest_data.py ingest_data.py

ENTRYPOINT [ "python", "ingest_data.py" ]
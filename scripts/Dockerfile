# Stage 1: Build stage
FROM python:3.12-slim AS builder

RUN apt-get update && apt-get install -y --no-install-recommends build-essential gcc \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /install

RUN pip install --upgrade pip && pip install --prefix=/install --no-cache-dir google-cloud-storage

# Stage 2: Final image
FROM python:3.12-slim

COPY --from=builder /install /usr/local

WORKDIR /app

COPY scripts/ingest_data.py ingest_data.py

ENTRYPOINT ["python", "ingest_data.py"]
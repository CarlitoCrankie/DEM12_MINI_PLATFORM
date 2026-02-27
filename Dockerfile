# ──────────────────────────────────────────────────────────────
#  Mini Data Platform — Multi-Stage Dockerfile
#
#  Stages:
#    base      → shared Python + system deps
#    airflow   → Airflow with extra providers
#    generator → lightweight data generator
# ──────────────────────────────────────────────────────────────

# ── Base stage: shared system deps ────────────────────────────
FROM python:3.11-slim AS base

RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /requirements.txt

# ── Airflow stage ──────────────────────────────────────────────
FROM apache/airflow:2.8.1-python3.11 AS airflow

USER root
RUN apt-get update && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir \
        apache-airflow-providers-postgres==5.10.0 \
        apache-airflow-providers-amazon==8.18.0 \
        minio==7.2.3 \
        pandas==2.2.0 \
        psycopg2-binary==2.9.9

# ── Data Generator stage ───────────────────────────────────────
FROM base AS generator

WORKDIR /app
RUN pip install --no-cache-dir \
        faker==22.2.0 \
        minio==7.2.3 \
        pandas==2.2.0

COPY data-generator/generate_data.py .

CMD ["python", "generate_data.py"]

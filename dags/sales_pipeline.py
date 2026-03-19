"""
Mini Data Platform — Sales Processing Pipeline
All connection details are read from environment variables set via .env → docker-compose.
"""

from __future__ import annotations

import io
import logging
import os
from datetime import timedelta

import pandas as pd
import psycopg2
from minio import Minio
from minio.commonconfig import CopySource
from minio.error import S3Error

from airflow import DAG
from airflow.decorators import task
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)

# ── Config from environment ────────────────────────────────────────────────────
def _env(key: str) -> str:
    value = os.environ.get(key)
    if not value:
        raise EnvironmentError(f"Required environment variable '{key}' is not set.")
    return value

MINIO_ENDPOINT   = _env("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = _env("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = _env("MINIO_SECRET_KEY")
SOURCE_BUCKET    = _env("MINIO_SOURCE_BUCKET")
ARCHIVE_BUCKET   = _env("MINIO_ARCHIVE_BUCKET")

PG_CONFIG = {
    "host":     _env("POSTGRES_HOST"),
    "port":     int(_env("POSTGRES_PORT")),
    "dbname":   _env("POSTGRES_DB"),
    "user":     _env("POSTGRES_USER"),
    "password": _env("POSTGRES_PASSWORD"),
}

REQUIRED_COLUMNS = {
    "order_id", "sale_date", "customer_name", "customer_email",
    "region", "channel", "product_id", "product_name", "category",
    "quantity", "unit_price", "discount_pct", "total_amount",
}


def get_minio() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def get_pg():
    return psycopg2.connect(**PG_CONFIG)


default_args = {
    "owner":            "data-platform",
    "retries":          2,
    "retry_delay":      timedelta(minutes=2),
    "email_on_failure": False,
}

with DAG(
    dag_id="sales_pipeline",
    default_args=default_args,
    description="MinIO (raw CSV) -> clean -> PostgreSQL -> archive",
    schedule_interval="@hourly",
    start_date=days_ago(1),
    catchup=False,
    tags=["sales", "etl"],
) as dag:

    @task
    def list_new_files() -> list:
        mc = get_minio()
        new_files = [
            obj.object_name
            for obj in mc.list_objects(SOURCE_BUCKET, prefix="raw/", recursive=True)
            if obj.object_name.endswith(".csv")
        ]
        log.info("%d new file(s) found.", len(new_files))
        return new_files

    @task
    def validate_files(file_list: list) -> list:
        if not file_list:
            return []
        mc    = get_minio()
        valid = []
        for obj in file_list:
            try:
                resp = mc.get_object(SOURCE_BUCKET, obj)
                df   = pd.read_csv(io.BytesIO(resp.read()))
                resp.close()
                missing = REQUIRED_COLUMNS - set(df.columns)
                if missing:
                    log.warning("Skipping %s — missing columns: %s", obj, missing)
                    continue
                if df.empty:
                    log.warning("Skipping %s — file is empty.", obj)
                    continue
                valid.append(obj)
                log.info("Validated %s (%d rows)", obj, len(df))
            except Exception as exc:
                log.error("Validation error for %s: %s", obj, exc)
        return valid

    @task
    def process_and_load(file_list: list) -> list:
        if not file_list:
            return []
        mc      = get_minio()
        results = []
        for obj in file_list:
            try:
                resp = mc.get_object(SOURCE_BUCKET, obj)
                df   = pd.read_csv(io.BytesIO(resp.read()))
                resp.close()

                df = df.dropna(subset=["order_id", "sale_date", "product_id"])
                df["sale_date"]    = pd.to_datetime(df["sale_date"], errors="coerce").dt.date
                df["quantity"]     = pd.to_numeric(df["quantity"],     errors="coerce").fillna(1).astype(int)
                df["unit_price"]   = pd.to_numeric(df["unit_price"],   errors="coerce").fillna(0)
                df["discount_pct"] = pd.to_numeric(df["discount_pct"], errors="coerce").fillna(0)
                df["region"]       = df["region"].str.strip().fillna("Unknown")
                df["channel"]      = df["channel"].str.strip().fillna("Unknown")
                df = df.dropna(subset=["sale_date"])

                rows_loaded = 0
                with get_pg() as conn, conn.cursor() as cur:
                    for _, row in df.iterrows():
                        cur.execute("""
                            INSERT INTO dim_customers (customer_name, email, region)
                            VALUES (%s, %s, %s)
                            ON CONFLICT (email) DO UPDATE
                              SET customer_name = EXCLUDED.customer_name,
                                  region        = EXCLUDED.region
                            RETURNING customer_id
                        """, (row["customer_name"], row["customer_email"], row["region"]))
                        customer_id = cur.fetchone()[0]

                        cur.execute("""
                            INSERT INTO dim_products (product_id, product_name, category, unit_price)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (product_id) DO UPDATE
                              SET product_name = EXCLUDED.product_name,
                                  category     = EXCLUDED.category,
                                  unit_price   = EXCLUDED.unit_price
                            RETURNING product_id
                        """, (int(row["product_id"]), row["product_name"], row["category"], float(row["unit_price"])))
                        product_id = cur.fetchone()[0]

                        cur.execute("""
                            INSERT INTO fact_sales
                              (order_id, sale_date, customer_id, product_id,
                               quantity, unit_price, discount, region, channel, source_file)
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (order_id) DO NOTHING
                        """, (
                            str(row["order_id"]),
                            row["sale_date"],
                            customer_id,
                            product_id,
                            int(row["quantity"]),
                            float(row["unit_price"]),
                            float(row["discount_pct"]),
                            row["region"],
                            row["channel"],
                            obj,
                        ))
                        rows_loaded += cur.rowcount
                    conn.commit()

                log.info("Loaded %d rows from %s", rows_loaded, obj)
                results.append({"file": obj, "rows": rows_loaded, "status": "success"})
            except Exception as exc:
                log.error("Failed to process %s: %s", obj, exc)
                results.append({"file": obj, "rows": 0, "status": "error", "error": str(exc)})
        return results

    @task
    def refresh_materialized_views(results: list) -> None:
        if not any(r["status"] == "success" for r in results):
            log.info("No successful loads — skipping view refresh.")
            return
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY mv_monthly_sales")
            conn.commit()
        log.info("Refreshed mv_monthly_sales.")

    @task
    def archive_files(results: list) -> None:
        mc = get_minio()
        if not mc.bucket_exists(ARCHIVE_BUCKET):
            mc.make_bucket(ARCHIVE_BUCKET)
        for r in results:
            if r["status"] != "success":
                continue
            src     = r["file"]
            archive = src.replace("raw/", "archive/")
            try:
                mc.copy_object(ARCHIVE_BUCKET, archive, CopySource(SOURCE_BUCKET, src))
                mc.remove_object(SOURCE_BUCKET, src)
                log.info("Archived %s -> %s/%s", src, ARCHIVE_BUCKET, archive)
            except S3Error as exc:
                log.error("Archive failed for %s: %s", src, exc)

    @task
    def log_runs(results: list) -> None:
        if not results:
            return
        with get_pg() as conn, conn.cursor() as cur:
            for r in results:
                cur.execute("""
                    INSERT INTO pipeline_runs (dag_id, source_file, rows_loaded, status, error_message)
                    VALUES (%s, %s, %s, %s, %s)
                """, ("sales_pipeline", r["file"], r["rows"], r["status"], r.get("error")))
            conn.commit()
        log.info("Logged %d run record(s).", len(results))

    # Wire up
    files   = list_new_files()
    valid   = validate_files(files)
    results = process_and_load(valid)
    refresh_materialized_views(results)
    archive_files(results)
    log_runs(results)

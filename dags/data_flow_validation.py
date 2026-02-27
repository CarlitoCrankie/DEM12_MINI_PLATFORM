"""
Mini Data Platform — Data Flow Validation DAG
Validates every hop in the pipeline: MinIO → Airflow → PostgreSQL → views.
Triggered manually or by CI/CD — not scheduled.
"""

from __future__ import annotations

import io
import logging
import os
from datetime import timedelta

import psycopg2
from minio import Minio

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

PG_CONFIG = {
    "host":     _env("POSTGRES_HOST"),
    "port":     int(_env("POSTGRES_PORT")),
    "dbname":   _env("POSTGRES_DB"),
    "user":     _env("POSTGRES_USER"),
    "password": _env("POSTGRES_PASSWORD"),
}


def get_minio() -> Minio:
    endpoint = MINIO_ENDPOINT.replace("http://", "").replace("https://", "")
    return Minio(endpoint, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)


def get_pg():
    return psycopg2.connect(**PG_CONFIG)


default_args = {
    "owner":            "data-platform",
    "retries":          1,
    "retry_delay":      timedelta(minutes=1),
    "email_on_failure": False,
}

with DAG(
    dag_id="data_flow_validation",
    default_args=default_args,
    description="End-to-end validation: MinIO → Airflow → PostgreSQL → views",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
    tags=["validation", "ci"],
) as dag:

    @task
    def check_minio() -> dict:
        mc      = get_minio()
        buckets = [b.name for b in mc.list_buckets()]
        assert SOURCE_BUCKET in buckets, f"Bucket '{SOURCE_BUCKET}' missing. Found: {buckets}"
        count = sum(1 for _ in mc.list_objects(SOURCE_BUCKET, recursive=True))
        log.info("MinIO ✓  bucket=%s  objects=%d", SOURCE_BUCKET, count)
        return {"status": "ok", "bucket": SOURCE_BUCKET, "object_count": count}


    @task
    def check_postgres_schema() -> dict:
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = [r[0] for r in cur.fetchall()]

        required = {"fact_sales", "dim_products", "dim_customers", "pipeline_runs"}
        missing  = required - set(tables)
        assert not missing, f"Missing tables: {missing}"

        log.info("PostgreSQL ✓  tables=%s", tables)
        return {"status": "ok", "tables": tables}


    @task
    def check_data_counts() -> dict:
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_sales")
            sales = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_customers")
            customers = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM dim_products")
            products = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status = 'success'")
            runs = cur.fetchone()[0]

        assert products > 0, "No products seeded — check init_db.sql"
        log.info("Rows — fact_sales=%d  customers=%d  products=%d  pipeline_runs=%d",
                 sales, customers, products, runs)
        return {"fact_sales": sales, "customers": customers, "products": products, "pipeline_runs": runs}


    @task
    def check_materialized_view() -> dict:
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mv_monthly_sales")
            count = cur.fetchone()[0]
        log.info("mv_monthly_sales rows: %d", count)
        return {"mv_monthly_sales_rows": count}


    @task
    def check_minio_roundtrip() -> str:
        """Upload a tiny test CSV, read it back, then delete it."""
        test_csv = (
            "order_id,sale_date,customer_name,customer_email,region,channel,"
            "product_id,product_name,category,quantity,unit_price,discount_pct,total_amount\n"
            "TEST0001,2024-01-01,Test User,test@mdp.local,North,Online,"
            "1,Laptop Pro 15,Electronics,1,1299.99,0,1299.99\n"
        )
        mc   = get_minio()
        key  = "validation/roundtrip_test.csv"
        data = test_csv.encode()
        mc.put_object(SOURCE_BUCKET, key, io.BytesIO(data), len(data), content_type="text/csv")

        resp    = mc.get_object(SOURCE_BUCKET, key)
        content = resp.read().decode()
        resp.close()
        mc.remove_object(SOURCE_BUCKET, key)

        assert "TEST0001" in content, "Round-trip content mismatch"
        log.info("MinIO round-trip ✓")
        return "ok"


    @task
    def summarise(minio: dict, schema: dict, counts: dict, view: dict, roundtrip: str) -> None:
        log.info("=" * 55)
        log.info("  DATA FLOW VALIDATION — SUMMARY")
        log.info("=" * 55)
        log.info("MinIO         : %s  (%d objects)",   minio["status"],  minio["object_count"])
        log.info("PG Schema     : %s  (%d tables)",    schema["status"], len(schema["tables"]))
        log.info("fact_sales    : %d rows",             counts["fact_sales"])
        log.info("dim_customers : %d rows",             counts["customers"])
        log.info("Monthly view  : %d rows",             view["mv_monthly_sales_rows"])
        log.info("MinIO R/T     : %s",                  roundtrip)
        log.info("=" * 55)
        log.info("ALL CHECKS PASSED ✓")


    # Wire tasks
    m  = check_minio()
    s  = check_postgres_schema()
    c  = check_data_counts()
    v  = check_materialized_view()
    rt = check_minio_roundtrip()
    summarise(m, s, c, v, rt)

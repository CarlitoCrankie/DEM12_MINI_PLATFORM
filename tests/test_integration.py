"""
tests/test_integration.py
Integration tests that verify data flows correctly through the platform.
Requires all services running: docker compose up -d

Run with: python -m pytest tests/test_integration.py -v -s
"""

import io
import os
import pytest

# ── Load .env if present ───────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))
except ImportError:
    pass

# ── Connection config ──────────────────────────────────────────────────────────
# When running outside Docker, internal hostnames (postgres, minio) don't work.
# We always use localhost for local test runs.
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER",       "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD",   "minio123")
MINIO_PORT       = os.getenv("MINIO_API_PORT",        "9000")
MINIO_ENDPOINT   = f"localhost:{MINIO_PORT}"
SOURCE_BUCKET    = os.getenv("MINIO_SOURCE_BUCKET",   "sales-data")

PG = {
    "host":     "localhost",
    "port":     int(os.getenv("POSTGRES_PORT",          "5432")),
    "dbname":   os.getenv("SALES_DB",                   "salesdb"),
    "user":     os.getenv("POSTGRES_ADMIN_USER",        "admin"),
    "password": os.getenv("POSTGRES_ADMIN_PASSWORD",    "adminpass"),
}


def get_minio():
    from minio import Minio
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def get_pg():
    import psycopg2
    return psycopg2.connect(**PG)


def services_available():
    try:
        get_minio().list_buckets()
    except Exception as exc:
        print(f"\n[integration skip] MinIO unreachable at {MINIO_ENDPOINT}: {exc}")
        return False
    try:
        get_pg().close()
    except Exception as exc:
        print(f"\n[integration skip] PostgreSQL unreachable at localhost:{PG['port']}: {exc}")
        return False
    return True


_services_up = services_available()

skip_if_no_services = pytest.mark.skipif(
    not _services_up,
    reason="Platform services not running — start with: docker compose up -d"
)


@skip_if_no_services
class TestMinIOConnectivity:

    def test_minio_reachable(self):
        assert len(get_minio().list_buckets()) > 0

    def test_source_bucket_exists(self):
        assert get_minio().bucket_exists(SOURCE_BUCKET)

    def test_csv_upload_download_roundtrip(self):
        mc  = get_minio()
        key = "test/integration_roundtrip.csv"
        csv = b"col1,col2\nval1,val2\n"
        mc.put_object(SOURCE_BUCKET, key, io.BytesIO(csv), len(csv), content_type="text/csv")
        resp    = mc.get_object(SOURCE_BUCKET, key)
        content = resp.read()
        resp.close()
        mc.remove_object(SOURCE_BUCKET, key)
        assert content == csv

    def test_raw_prefix_has_files(self):
        files = list(get_minio().list_objects(SOURCE_BUCKET, prefix="raw/", recursive=True))
        assert len(files) > 0, "No files in raw/ — run: docker compose run --rm data-generator"


@skip_if_no_services
class TestPostgreSQLSchema:

    def test_postgres_reachable(self):
        get_pg().close()

    def test_required_tables_exist(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT table_name FROM information_schema.tables
                WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            """)
            tables = {row[0] for row in cur.fetchall()}
        missing = {"fact_sales", "dim_products", "dim_customers", "pipeline_runs"} - tables
        assert not missing, f"Missing tables: {missing}"

    def test_materialized_view_exists(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT matviewname FROM pg_matviews WHERE schemaname='public'")
            assert "mv_monthly_sales" in {r[0] for r in cur.fetchall()}

    def test_analytical_views_exist(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT viewname FROM pg_views WHERE schemaname='public'")
            views = {r[0] for r in cur.fetchall()}
        assert "v_product_performance" in views
        assert "v_regional_performance" in views

    def test_products_seeded(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM dim_products")
            assert cur.fetchone()[0] >= 14

    def test_fact_sales_has_data(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_sales")
            assert cur.fetchone()[0] > 0, "fact_sales empty — trigger the pipeline"

    def test_fact_sales_no_null_order_ids(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_sales WHERE order_id IS NULL")
            assert cur.fetchone()[0] == 0

    def test_fact_sales_no_negative_amounts(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM fact_sales WHERE total_amount < 0")
            assert cur.fetchone()[0] == 0

    def test_pipeline_runs_logged(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM pipeline_runs WHERE status='success'")
            assert cur.fetchone()[0] > 0, "No successful pipeline runs recorded"


@skip_if_no_services
class TestDataIntegrity:

    def test_all_sales_have_valid_customers(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fact_sales fs
                LEFT JOIN dim_customers c USING (customer_id)
                WHERE c.customer_id IS NULL
            """)
            assert cur.fetchone()[0] == 0

    def test_all_sales_have_valid_products(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM fact_sales fs
                LEFT JOIN dim_products p USING (product_id)
                WHERE p.product_id IS NULL
            """)
            assert cur.fetchone()[0] == 0

    def test_no_duplicate_order_ids(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM (
                    SELECT order_id FROM fact_sales
                    GROUP BY order_id HAVING COUNT(*) > 1
                ) dupes
            """)
            assert cur.fetchone()[0] == 0

    def test_monthly_view_populated_after_pipeline(self):
        with get_pg() as conn, conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM mv_monthly_sales")
            assert cur.fetchone()[0] > 0

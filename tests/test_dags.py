"""
tests/test_dags.py
Tests for Airflow DAG structure and task configuration.

NOTE: Airflow does not support Windows. These tests are skipped locally
on Windows and run automatically in CI via GitHub Actions (Linux).

Run with: python -m pytest tests/test_dags.py -v
"""

import os
import sys
import platform
import pytest

# Skip entire module on Windows
if platform.system() == "Windows":
    pytest.skip(
        "Airflow does not support Windows. "
        "These tests run in CI on Linux. "
        "See: https://github.com/apache/airflow/issues/10388",
        allow_module_level=True
    )

# SQLite path must be absolute and platform-aware
import tempfile
_tmp_db = os.path.join(tempfile.gettempdir(), "test_airflow.db")
_sqlite_conn = f"sqlite:///{_tmp_db}"

os.environ.setdefault("AIRFLOW__CORE__EXECUTOR",             "LocalExecutor")
os.environ.setdefault("AIRFLOW__DATABASE__SQL_ALCHEMY_CONN", _sqlite_conn)
os.environ.setdefault("AIRFLOW__CORE__FERNET_KEY",           "81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=")
os.environ.setdefault("MINIO_ENDPOINT",                      "http://minio:9000")
os.environ.setdefault("MINIO_ACCESS_KEY",                    "minio")
os.environ.setdefault("MINIO_SECRET_KEY",                    "minio123")
os.environ.setdefault("MINIO_SOURCE_BUCKET",                 "sales-data")
os.environ.setdefault("MINIO_ARCHIVE_BUCKET",                "processed-data")
os.environ.setdefault("POSTGRES_HOST",                       "postgres")
os.environ.setdefault("POSTGRES_PORT",                       "5432")
os.environ.setdefault("POSTGRES_DB",                         "salesdb")
os.environ.setdefault("POSTGRES_USER",                       "datauser")
os.environ.setdefault("POSTGRES_PASSWORD",                   "datapass")

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'dags'))


class TestSalesPipelineDag:

    @pytest.fixture(scope="class")
    def dag(self):
        from sales_pipeline import dag
        return dag

    def test_dag_id(self, dag):
        assert dag.dag_id == "sales_pipeline"

    def test_dag_has_correct_schedule(self, dag):
        assert dag.schedule == "@hourly"

    def test_dag_tags(self, dag):
        assert "sales" in dag.tags
        assert "etl" in dag.tags

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_expected_tasks_present(self, dag):
        expected = {
            "list_new_files", "validate_files", "process_and_load",
            "refresh_materialized_views", "archive_files", "log_runs",
        }
        assert expected == {t.task_id for t in dag.tasks}

    def test_task_count(self, dag):
        assert len(dag.tasks) == 6

    def test_list_new_files_has_no_upstream(self, dag):
        assert len(dag.get_task("list_new_files").upstream_task_ids) == 0

    def test_validate_files_upstream(self, dag):
        assert "list_new_files" in dag.get_task("validate_files").upstream_task_ids

    def test_process_and_load_upstream(self, dag):
        assert "validate_files" in dag.get_task("process_and_load").upstream_task_ids

    def test_downstream_tasks_after_load(self, dag):
        downstream = dag.get_task("process_and_load").downstream_task_ids
        assert "refresh_materialized_views" in downstream
        assert "archive_files" in downstream
        assert "log_runs" in downstream

    def test_retries_configured(self, dag):
        for task in dag.tasks:
            assert task.retries >= 1

    def test_owner_set(self, dag):
        for task in dag.tasks:
            assert task.owner == "data-platform"


class TestValidationDag:

    @pytest.fixture(scope="class")
    def dag(self):
        from data_flow_validation import dag
        return dag

    def test_dag_id(self, dag):
        assert dag.dag_id == "data_flow_validation"

    def test_no_schedule(self, dag):
        assert dag.schedule_interval is None

    def test_dag_tags(self, dag):
        assert "validation" in dag.tags
        assert "ci" in dag.tags

    def test_catchup_disabled(self, dag):
        assert dag.catchup is False

    def test_expected_tasks_present(self, dag):
        expected = {
            "check_minio", "check_postgres_schema", "check_data_counts",
            "check_materialized_view", "check_minio_roundtrip", "summarise",
        }
        assert expected == {t.task_id for t in dag.tasks}

    def test_summarise_has_all_upstreams(self, dag):
        upstream = dag.get_task("summarise").upstream_task_ids
        assert "check_minio" in upstream
        assert "check_postgres_schema" in upstream
        assert "check_data_counts" in upstream
        assert "check_materialized_view" in upstream
        assert "check_minio_roundtrip" in upstream

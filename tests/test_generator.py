"""
tests/test_generator.py
Tests for the data generator module.
Run with: pytest tests/test_generator.py -v
"""

import io
import os
import sys
import pytest
import pandas as pd
from unittest.mock import MagicMock, patch
from datetime import datetime, timedelta

# Make the generator importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'data-generator'))

# ── Set required env vars before importing module ──────────────────────────────
os.environ.setdefault("MINIO_ENDPOINT",      "http://minio:9000")
os.environ.setdefault("MINIO_ACCESS_KEY",    "minio")
os.environ.setdefault("MINIO_SECRET_KEY",    "minio123")
os.environ.setdefault("MINIO_SOURCE_BUCKET", "sales-data")

import generate_data as gen


class TestGenerateBatch:
    """Tests for the generate_batch function."""

    def test_returns_dataframe(self):
        start = datetime(2025, 1, 1)
        end   = datetime(2025, 1, 31)
        df    = gen.generate_batch(10, start, end)
        assert isinstance(df, pd.DataFrame)

    def test_correct_row_count(self):
        start = datetime(2025, 1, 1)
        end   = datetime(2025, 1, 31)
        df    = gen.generate_batch(50, start, end)
        assert len(df) == 50

    def test_required_columns_present(self):
        required = {
            "order_id", "sale_date", "customer_name", "customer_email",
            "region", "channel", "product_id", "product_name", "category",
            "quantity", "unit_price", "discount_pct", "total_amount",
        }
        df = gen.generate_batch(5, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert required.issubset(set(df.columns))

    def test_no_null_critical_fields(self):
        df = gen.generate_batch(20, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert df["order_id"].isnull().sum() == 0
        assert df["sale_date"].isnull().sum() == 0
        assert df["product_id"].isnull().sum() == 0

    def test_valid_regions(self):
        df = gen.generate_batch(50, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert df["region"].isin(["North", "South", "East", "West", "Central"]).all()

    def test_valid_channels(self):
        df = gen.generate_batch(50, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert df["channel"].isin(["Online", "In-Store", "Wholesale", "Partner"]).all()

    def test_quantity_positive(self):
        df = gen.generate_batch(30, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert (df["quantity"] > 0).all()

    def test_unit_price_positive(self):
        df = gen.generate_batch(30, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert (df["unit_price"] > 0).all()

    def test_total_amount_positive(self):
        df = gen.generate_batch(30, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert (df["total_amount"] >= 0).all()

    def test_discount_valid_range(self):
        df = gen.generate_batch(100, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert (df["discount_pct"] >= 0).all()
        assert (df["discount_pct"] <= 100).all()

    def test_unique_order_ids(self):
        """Each run should produce unique order IDs (no fixed seed)."""
        df1 = gen.generate_batch(20, datetime(2025, 1, 1), datetime(2025, 1, 31))
        df2 = gen.generate_batch(20, datetime(2025, 1, 1), datetime(2025, 1, 31))
        overlap = set(df1["order_id"]) & set(df2["order_id"])
        assert len(overlap) == 0, f"Duplicate order_ids found across runs: {overlap}"

    def test_product_ids_in_catalogue(self):
        valid_ids = {p["id"] for p in gen.PRODUCTS}
        df = gen.generate_batch(50, datetime(2025, 1, 1), datetime(2025, 1, 31))
        assert df["product_id"].isin(valid_ids).all()

    def test_sale_dates_in_range(self):
        start = datetime(2025, 6, 1)
        end   = datetime(2025, 6, 30)
        df    = gen.generate_batch(30, start, end)
        dates = pd.to_datetime(df["sale_date"])
        assert (dates >= pd.Timestamp(start)).all()
        assert (dates <= pd.Timestamp(end)).all()


class TestUploadCsv:
    """Tests for the upload_csv function using a mocked MinIO client."""

    def test_upload_called_with_correct_bucket(self):
        mock_client = MagicMock()
        df = gen.generate_batch(5, datetime(2025, 1, 1), datetime(2025, 1, 31))
        gen.upload_csv(mock_client, df, "test_file.csv")
        mock_client.put_object.assert_called_once()
        call_kwargs = mock_client.put_object.call_args
        assert call_kwargs[1]["bucket_name"] == gen.MINIO_BUCKET or \
               call_kwargs[0][0] == gen.MINIO_BUCKET

    def test_upload_correct_object_name(self):
        mock_client = MagicMock()
        df = gen.generate_batch(5, datetime(2025, 1, 1), datetime(2025, 1, 31))
        gen.upload_csv(mock_client, df, "sales_2025_01.csv")
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["object_name"] == "raw/sales_2025_01.csv"

    def test_upload_csv_content_type(self):
        mock_client = MagicMock()
        df = gen.generate_batch(5, datetime(2025, 1, 1), datetime(2025, 1, 31))
        gen.upload_csv(mock_client, df, "test.csv")
        call_kwargs = mock_client.put_object.call_args[1]
        assert call_kwargs["content_type"] == "text/csv"


class TestProductCatalogue:
    """Tests for the static product catalogue."""

    def test_all_products_have_required_keys(self):
        for p in gen.PRODUCTS:
            assert "id" in p
            assert "name" in p
            assert "category" in p
            assert "price" in p

    def test_product_prices_positive(self):
        for p in gen.PRODUCTS:
            assert p["price"] > 0, f"Product {p['name']} has non-positive price"

    def test_product_ids_unique(self):
        ids = [p["id"] for p in gen.PRODUCTS]
        assert len(ids) == len(set(ids)), "Duplicate product IDs found"

    def test_valid_categories(self):
        valid = {"Electronics", "Furniture", "Stationery", "Appliances"}
        for p in gen.PRODUCTS:
            assert p["category"] in valid, f"Unknown category: {p['category']}"

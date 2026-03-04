#!/usr/bin/env python3
"""
Mini Data Platform — Sales Data Generator
All configuration is read from environment variables (set via .env → docker-compose).
"""

import io
import os
import random
import time
import logging
from datetime import datetime, timedelta

import pandas as pd
from faker import Faker
from minio import Minio

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Config from environment ────────────────────────────────────────────────────
MINIO_ENDPOINT   = os.environ["MINIO_ENDPOINT"].replace("http://", "").replace("https://", "")
MINIO_ACCESS_KEY = os.environ["MINIO_ACCESS_KEY"]
MINIO_SECRET_KEY = os.environ["MINIO_SECRET_KEY"]
MINIO_BUCKET     = os.environ["MINIO_SOURCE_BUCKET"]

SECURE = os.getenv("MINIO_SECURE", "false").lower() == "true"

# ── Static product catalogue (mirrors DB seed) ────────────────────────────────
PRODUCTS = [
    {"id": 1,  "name": "Laptop Pro 15",       "category": "Electronics", "price": 1299.99},
    {"id": 2,  "name": "Wireless Mouse",       "category": "Electronics", "price":   29.99},
    {"id": 3,  "name": "USB-C Hub",            "category": "Electronics", "price":   49.99},
    {"id": 4,  "name": "Mechanical Keyboard",  "category": "Electronics", "price":  129.99},
    {"id": 5,  "name": "4K Monitor",           "category": "Electronics", "price":  399.99},
    {"id": 6,  "name": "Office Chair",         "category": "Furniture",   "price":  299.99},
    {"id": 7,  "name": "Standing Desk",        "category": "Furniture",   "price":  599.99},
    {"id": 8,  "name": "Bookshelf",            "category": "Furniture",   "price":  149.99},
    {"id": 9,  "name": "Notebook Set",         "category": "Stationery",  "price":   12.99},
    {"id": 10, "name": "Pen Pack (10)",        "category": "Stationery",  "price":    8.99},
    {"id": 11, "name": "Whiteboard",           "category": "Stationery",  "price":   79.99},
    {"id": 12, "name": "Coffee Maker",         "category": "Appliances",  "price":  129.99},
    {"id": 13, "name": "Water Purifier",       "category": "Appliances",  "price":  199.99},
    {"id": 14, "name": "Air Purifier",         "category": "Appliances",  "price":  249.99},
]

REGIONS  = ["North", "South", "East", "West", "Central"]
CHANNELS = ["Online", "In-Store", "Wholesale", "Partner"]

fake = Faker()
# No fixed seed — each run generates unique order IDs


def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=SECURE)


def wait_for_minio(client: Minio, retries: int = 12, delay: int = 5) -> None:
    for attempt in range(retries):
        try:
            client.list_buckets()
            log.info("MinIO is ready.")
            return
        except Exception as exc:
            log.warning("MinIO not ready (attempt %d/%d): %s", attempt + 1, retries, exc)
            time.sleep(delay)
    raise RuntimeError("MinIO did not become ready in time.")


def generate_batch(n: int, start: datetime, end: datetime) -> pd.DataFrame:
    rows = []
    for _ in range(n):
        p        = random.choice(PRODUCTS)
        qty      = random.randint(1, 10)
        discount = random.choice([0, 0, 0, 5, 10, 15, 20])
        date     = fake.date_time_between(start_date=start, end_date=end)
        rows.append({
            "order_id":       fake.uuid4()[:8].upper(),
            "sale_date":      date.strftime("%Y-%m-%d"),
            "customer_name":  fake.name(),
            "customer_email": fake.email(),
            "region":         random.choice(REGIONS),
            "channel":        random.choice(CHANNELS),
            "product_id":     p["id"],
            "product_name":   p["name"],
            "category":       p["category"],
            "quantity":       qty,
            "unit_price":     p["price"],
            "discount_pct":   discount,
            "total_amount":   round(qty * p["price"] * (1 - discount / 100), 2),
        })
    return pd.DataFrame(rows)


def upload_csv(client: Minio, df: pd.DataFrame, object_name: str) -> None:
    data  = df.to_csv(index=False).encode("utf-8")
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=f"raw/{object_name}",
        data=io.BytesIO(data),
        length=len(data),
        content_type="text/csv",
    )
    log.info("Uploaded raw/%s  (%d rows, %.1f KB)", object_name, len(df), len(data) / 1024)


def main() -> None:
    log.info("Starting data generator …")
    log.info("MinIO endpoint : %s", MINIO_ENDPOINT)
    log.info("Target bucket  : %s", MINIO_BUCKET)

    client = get_minio_client()
    wait_for_minio(client)

    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)

    today = datetime.now()

    # Use a run timestamp so every generator run produces unique filenames
    run_ts = today.strftime('%Y%m%d_%H%M%S')

    # 12 monthly historical files
    for offset in range(12, 0, -1):
        start = today.replace(day=1) - timedelta(days=30 * offset)
        end   = start + timedelta(days=29)
        df    = generate_batch(random.randint(150, 400), start, end)
        upload_csv(client, df, f"sales_{start.strftime('%Y_%m')}_{run_ts}.csv")

    # Live / today file
    df_today = generate_batch(50, today - timedelta(hours=6), today)
    upload_csv(client, df_today, f"sales_{today.strftime('%Y_%m_%d')}_{run_ts}_latest.csv")

    log.info("Done — 13 files uploaded to MinIO bucket '%s/raw/'", MINIO_BUCKET)


if __name__ == "__main__":
    main()
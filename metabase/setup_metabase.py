#!/usr/bin/env python3
"""
Mini Data Platform — Metabase Auto-Setup Helper
================================================
Automates Metabase initial configuration via its REST API:
  1. Complete setup wizard (admin account + DB connection)
  2. Create database connection to PostgreSQL salesdb
  3. Create a collection and example questions/dashboard

Usage:
    python setup_metabase.py

Requirements:
    pip install requests
"""

import json
import time
import logging
import requests
import os

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

METABASE_URL  = os.environ.get("METABASE_URL")
ADMIN_EMAIL   = os.environ.get("METABASE_ADMIN_EMAIL")
ADMIN_PASS    = os.environ.get("METABASE_ADMIN_PASS")
ADMIN_FIRST   = os.environ.get("METABASE_ADMIN_FIRST")
ADMIN_LAST    = os.environ.get("METABASE_ADMIN_LAST")

PG_HOST       = os.environ.get("METABASE_PG_HOST")
PG_PORT       = int(os.environ.get("METABASE_PG_PORT", 5432))
PG_DB         = os.environ.get("METABASE_PG_DB")
PG_USER       = os.environ.get("METABASE_PG_USER")
PG_PASS       = os.environ.get("METABASE_PG_PASS")


def wait_for_metabase(timeout=300):
    """Poll until Metabase is ready."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = requests.get(f"{METABASE_URL}/api/health", timeout=5)
            if r.status_code == 200 and r.json().get("status") == "ok":
                log.info("Metabase is ready.")
                return
        except requests.RequestException:
            pass
        log.info("Waiting for Metabase …")
        time.sleep(5)
    raise TimeoutError("Metabase did not start in time.")


def get_setup_token():
    r = requests.get(f"{METABASE_URL}/api/session/properties")
    r.raise_for_status()
    return r.json().get("setup-token")


def complete_setup(token: str) -> str:
    """Run the Metabase setup wizard and return a session token."""
    payload = {
        "token": token,
        "user": {
            "first_name": ADMIN_FIRST,
            "last_name":  ADMIN_LAST,
            "email":      ADMIN_EMAIL,
            "password":   ADMIN_PASS,
            "site_name":  "Mini Data Platform",
        },
        "database": {
            "engine":  "postgres",
            "name":    "Sales DB",
            "details": {
                "host":     PG_HOST,
                "port":     PG_PORT,
                "dbname":   PG_DB,
                "user":     PG_USER,
                "password": PG_PASS,
            },
        },
        "invite":   None,
        "prefs": {
            "site_name":          "Mini Data Platform",
            "allow_tracking":     False,
            "site_locale":        "en",
        },
    }
    r = requests.post(f"{METABASE_URL}/api/setup", json=payload)
    r.raise_for_status()
    return r.json()["id"]


def login() -> str:
    r = requests.post(f"{METABASE_URL}/api/session", json={
        "username": ADMIN_EMAIL,
        "password": ADMIN_PASS,
    })
    r.raise_for_status()
    return r.json()["id"]


def get_db_id(session: str) -> int:
    headers = {"X-Metabase-Session": session}
    r = requests.get(f"{METABASE_URL}/api/database", headers=headers)
    r.raise_for_status()
    dbs = r.json()
    for db in (dbs.get("data") or dbs):
        if db.get("engine") == "postgres" and db.get("details", {}).get("dbname") == PG_DB:
            return db["id"]
    raise ValueError("Sales DB not found in Metabase")


def create_collection(session: str, name: str) -> int:
    headers = {"X-Metabase-Session": session}
    r = requests.post(f"{METABASE_URL}/api/collection", headers=headers, json={
        "name": name, "color": "#509EE3",
    })
    r.raise_for_status()
    return r.json()["id"]


def create_question(session: str, db_id: int, collection_id: int, name: str, query: dict) -> int:
    headers = {"X-Metabase-Session": session}
    r = requests.post(f"{METABASE_URL}/api/card", headers=headers, json={
        "name":            name,
        "display":         query.get("display", "table"),
        "collection_id":   collection_id,
        "dataset_query": {
            "database": db_id,
            "type":     "native",
            "native":   {"query": query["sql"]},
        },
        "visualization_settings": query.get("viz", {}),
    })
    r.raise_for_status()
    return r.json()["id"]


QUESTIONS = [
    {
        "name":    "Monthly Revenue Trend",
        "display": "line",
        "sql":     """
            SELECT month::date, SUM(total_revenue) AS revenue
            FROM mv_monthly_sales
            GROUP BY month
            ORDER BY month
        """,
        "viz": {
            "graph.dimensions": ["month"],
            "graph.metrics":    ["revenue"],
        },
    },
    {
        "name":    "Revenue by Region",
        "display": "bar",
        "sql":     """
            SELECT region, total_revenue, unique_customers
            FROM v_regional_performance
            ORDER BY total_revenue DESC
        """,
        "viz": {
            "graph.dimensions": ["region"],
            "graph.metrics":    ["total_revenue"],
        },
    },
    {
        "name":    "Top 10 Products by Revenue",
        "display": "bar",
        "sql":     """
            SELECT product_name, category, total_revenue, total_units_sold
            FROM v_product_performance
            ORDER BY total_revenue DESC
            LIMIT 10
        """,
        "viz": {
            "graph.dimensions": ["product_name"],
            "graph.metrics":    ["total_revenue"],
        },
    },
    {
        "name":    "Sales Channel Breakdown",
        "display": "pie",
        "sql":     """
            SELECT channel, COUNT(*) AS orders, SUM(total_amount) AS revenue
            FROM fact_sales
            GROUP BY channel
            ORDER BY revenue DESC
        """,
        "viz": {
            "pie.dimension": "channel",
            "pie.metric":    "revenue",
        },
    },
    {
        "name":    "Category Revenue by Month",
        "display": "bar",
        "sql":     """
            SELECT month::date, category, SUM(total_revenue) AS revenue
            FROM mv_monthly_sales
            GROUP BY month, category
            ORDER BY month, revenue DESC
        """,
        "viz": {
            "graph.dimensions": ["month", "category"],
            "graph.metrics":    ["revenue"],
        },
    },
]


def main():
    wait_for_metabase()

    # Try setup wizard; skip if already done
    setup_token = get_setup_token()
    if setup_token:
        log.info("Running Metabase setup wizard …")
        complete_setup(setup_token)
        time.sleep(3)

    session = login()
    log.info("Logged in to Metabase.")

    db_id = get_db_id(session)
    log.info("Sales DB ID: %d", db_id)

    col_id = create_collection(session, "Sales Analytics")
    log.info("Created collection 'Sales Analytics' (id=%d)", col_id)

    for q in QUESTIONS:
        qid = create_question(session, db_id, col_id, q["name"], q)
        log.info("Created question: %s (id=%d)", q["name"], qid)

    log.info("")
    log.info("✅  Metabase setup complete!")
    log.info("    Open http://localhost:3000 and log in with:")
    log.info("    Email:    %s", ADMIN_EMAIL)
    log.info("    Password: %s", ADMIN_PASS)


if __name__ == "__main__":
    main()

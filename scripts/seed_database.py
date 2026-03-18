"""
Seed Database Script — PostgreSQL (Azure)
==========================================
Reads data/seed/master_data.csv and data/seed/dlu_metadata.csv and inserts
records into Azure PostgreSQL.

Tables created if they do not exist:
  "PII"."master_data"           — customer PII records (customer_id INT PK)
  "DLU"."datalakeuniverse"      — breach-file metadata (MD5 PK + file_path)

Idempotent: existing rows (matched by primary key) are skipped via ON CONFLICT.

Usage:
    python scripts/seed_database.py

Environment variables:
    POSTGRES_SERVER   — PostgreSQL hostname
    POSTGRES_PORT     — PostgreSQL port (default: 5432)
    POSTGRES_DB       — Database name
    POSTGRES_USER     — Database username
    POSTGRES_PASSWORD — Database password
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_DIR = PROJECT_ROOT / "data" / "seed"
MASTER_DATA_CSV = SEED_DIR / "master_data.csv"
DLU_METADATA_CSV = SEED_DIR / "dlu_metadata.csv"

# ---------------------------------------------------------------------------
# Database connection helpers
# ---------------------------------------------------------------------------

def get_connection():
    """Return a live psycopg2 connection."""
    import psycopg2  # noqa: PLC0415 — lazy import

    server = os.getenv("POSTGRES_SERVER", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "datasense")
    user = os.getenv("POSTGRES_USER", "")
    password = os.getenv("POSTGRES_PASSWORD", "")

    return psycopg2.connect(
        host=server,
        port=int(port),
        dbname=database,
        user=user,
        password=password,
        sslmode="require",
    )


# ---------------------------------------------------------------------------
# Schema + table creation
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    # Schemas
    'CREATE SCHEMA IF NOT EXISTS "PII"',
    'CREATE SCHEMA IF NOT EXISTS "DLU"',

    # PII.master_data
    """
    CREATE TABLE IF NOT EXISTS "PII"."master_data" (
        customer_id     INTEGER        NOT NULL PRIMARY KEY,
        "Fullname"      VARCHAR(100)   NULL,
        "FirstName"     VARCHAR(50)    NULL,
        "LastName"      VARCHAR(50)    NULL,
        "DOB"           VARCHAR(20)    NULL,
        "SSN"           VARCHAR(15)    NULL,
        "DriversLicense" VARCHAR(30)   NULL,
        "Address1"      VARCHAR(200)   NULL,
        "Address2"      VARCHAR(200)   NULL,
        "Address3"      VARCHAR(200)   NULL,
        "ZipCode"       VARCHAR(10)    NULL,
        "City"          VARCHAR(100)   NULL,
        "State"         VARCHAR(5)     NULL,
        "Country"       VARCHAR(50)    NULL
    )
    """,

    # DLU.datalakeuniverse
    """
    CREATE TABLE IF NOT EXISTS "DLU"."datalakeuniverse" (
        "MD5"           VARCHAR(32)    NOT NULL PRIMARY KEY,
        file_path       VARCHAR(500)   NULL
    )
    """,
]


def create_schemas_and_tables(cursor) -> None:
    """Execute all DDL statements to create schemas and tables if needed."""
    for stmt in DDL_STATEMENTS:
        cursor.execute(stmt)
    cursor.connection.commit()
    logger.info("Schemas and tables verified/created.")
    print("  Schemas and tables verified/created.")


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_master_data(cursor) -> int:
    """
    Insert rows from master_data.csv into "PII"."master_data".
    Skips rows whose customer_id already exists (idempotent).
    Returns the count of rows inserted.
    """
    sql = """
    INSERT INTO "PII"."master_data"
        (customer_id, "Fullname", "FirstName", "LastName", "DOB", "SSN",
         "DriversLicense", "Address1", "Address2", "Address3",
         "ZipCode", "City", "State", "Country")
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (customer_id) DO NOTHING
    """
    inserted = 0
    with open(MASTER_DATA_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                int(row["customer_id"]),
                row["Fullname"], row["FirstName"], row["LastName"],
                row["DOB"], row["SSN"], row["DriversLicense"],
                row["Address1"], row["Address2"], row["Address3"],
                row["ZipCode"], row["City"], row["State"], row["Country"],
            ))
            inserted += cursor.rowcount
    cursor.connection.commit()
    return inserted


def seed_dlu_metadata(cursor) -> int:
    """
    Insert rows from dlu_metadata.csv into "DLU"."datalakeuniverse".
    Skips rows whose MD5 already exists (idempotent).
    Returns the count of rows inserted.
    """
    sql = """
    INSERT INTO "DLU"."datalakeuniverse" ("MD5", file_path)
    VALUES (%s, %s)
    ON CONFLICT ("MD5") DO NOTHING
    """
    inserted = 0
    with open(DLU_METADATA_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                row["MD5"],
                row["file_path"],
            ))
            inserted += cursor.rowcount
    cursor.connection.commit()
    return inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Connecting to PostgreSQL...")
    conn = get_connection()
    cursor = conn.cursor()

    print("Creating schemas and tables if needed...")
    create_schemas_and_tables(cursor)

    print('Seeding "PII"."master_data"...')
    n_master = seed_master_data(cursor)
    print(f"  {n_master} rows inserted into PII.master_data")

    print('Seeding "DLU"."datalakeuniverse"...')
    n_dlu = seed_dlu_metadata(cursor)
    print(f"  {n_dlu} rows inserted into DLU.datalakeuniverse")

    cursor.close()
    conn.close()
    print("Seeding complete.")


if __name__ == "__main__":
    main()

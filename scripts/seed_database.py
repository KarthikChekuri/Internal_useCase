"""
Phase V2-1.4 — Seed Database Script (V2 rewrite)
==================================================
Reads data/seed/master_data.csv and data/seed/dlu_metadata.csv and inserts
records into a local SQL Server instance.

V2 changes from V1:
  - Reads master_data.csv (not master_pii.csv)
  - Table is [PII].[master_data] (not [PII].[master_pii])
  - customer_id (INT) is the primary key for master_data
  - [DLU].[datalakeuniverse] uses MD5 as primary key (not GUID)
  - dlu_metadata.csv has only MD5 and file_path columns

  Tables created if they do not exist:
    [PII].[master_data]           — 10 customer PII records (customer_id INT PK)
    [DLU].[datalakeuniverse]      — breach-file metadata (MD5 PK + file_path)

  Idempotent: existing rows (matched by primary key) are skipped.

Usage:
    python scripts/seed_database.py

Environment variables (optional):
    DB_SERVER   — SQL Server hostname/instance  (default: localhost)
    DB_NAME     — Database name                  (default: BreachSearch)
    DB_USER     — SQL login user                 (default: Windows auth)
    DB_PASSWORD — SQL login password             (default: Windows auth)
"""

from __future__ import annotations

import csv
import logging
import os
from pathlib import Path

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

def _build_connection_string() -> str:
    server = os.getenv("DB_SERVER", "localhost,1433")
    database = os.getenv("DB_NAME", "BreachSearch")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")
    driver = os.getenv("DB_DRIVER", "SQL Server")

    if user and password:
        conn_str = (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={user};PWD={password}"
        )
        # ODBC Driver 17+ supports TrustServerCertificate; old "SQL Server" driver does not
        if "ODBC Driver" in driver:
            conn_str += ";TrustServerCertificate=yes"
        return conn_str
    # Windows integrated authentication
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes"
    )


def get_connection():
    """Return a live pyodbc connection (imported lazily)."""
    import pyodbc  # noqa: PLC0415 — lazy import avoids hang when not needed
    return pyodbc.connect(_build_connection_string())


# ---------------------------------------------------------------------------
# Schema + table creation
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    # Schemas
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'PII') EXEC('CREATE SCHEMA [PII]')",
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'DLU') EXEC('CREATE SCHEMA [DLU]')",

    # PII.master_data (V2: customer_id INT PK, 13 PII fields)
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'PII' AND t.name = 'master_data'
    )
    CREATE TABLE [PII].[master_data] (
        customer_id     INT            NOT NULL PRIMARY KEY,
        Fullname        NVARCHAR(100)  NULL,
        FirstName       NVARCHAR(50)   NULL,
        LastName        NVARCHAR(50)   NULL,
        DOB             NVARCHAR(20)   NULL,
        SSN             NVARCHAR(15)   NULL,
        DriversLicense  NVARCHAR(30)   NULL,
        Address1        NVARCHAR(200)  NULL,
        Address2        NVARCHAR(200)  NULL,
        Address3        NVARCHAR(200)  NULL,
        ZipCode         NVARCHAR(10)   NULL,
        City            NVARCHAR(100)  NULL,
        State           NVARCHAR(5)    NULL,
        Country         NVARCHAR(50)   NULL
    )
    """,

    # DLU.datalakeuniverse (V2: MD5 PK, only file_path)
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'DLU' AND t.name = 'datalakeuniverse'
    )
    CREATE TABLE [DLU].[datalakeuniverse] (
        MD5             NVARCHAR(32)   NOT NULL PRIMARY KEY,
        file_path       NVARCHAR(500)  NULL
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
    Insert rows from master_data.csv into [PII].[master_data].
    Skips rows whose customer_id already exists (idempotent).
    Returns the count of rows inserted.
    """
    sql = """
    IF NOT EXISTS (SELECT 1 FROM [PII].[master_data] WHERE customer_id = ?)
    INSERT INTO [PII].[master_data]
        (customer_id, Fullname, FirstName, LastName, DOB, SSN, DriversLicense,
         Address1, Address2, Address3, ZipCode, City, State, Country)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    inserted = 0
    with open(MASTER_DATA_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                int(row["customer_id"]),      # for the EXISTS check
                int(row["customer_id"]),      # INSERT value
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
    Insert rows from dlu_metadata.csv into [DLU].[datalakeuniverse].
    V2: uses MD5 as the primary key (not GUID).
    Skips rows whose MD5 already exists (idempotent).
    Returns the count of rows inserted.
    """
    sql = """
    IF NOT EXISTS (SELECT 1 FROM [DLU].[datalakeuniverse] WHERE MD5 = ?)
    INSERT INTO [DLU].[datalakeuniverse]
        (MD5, file_path)
    VALUES (?, ?)
    """
    inserted = 0
    with open(DLU_METADATA_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                row["MD5"],          # for the EXISTS check
                row["MD5"],          # INSERT value
                row["file_path"],
            ))
            inserted += cursor.rowcount
    cursor.connection.commit()
    return inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Connecting to SQL Server...")
    conn = get_connection()
    cursor = conn.cursor()

    print("Creating schemas and tables if needed...")
    create_schemas_and_tables(cursor)

    print("Seeding [PII].[master_data]...")
    n_master = seed_master_data(cursor)
    print(f"  {n_master} rows inserted into [PII].[master_data]")

    print("Seeding [DLU].[datalakeuniverse]...")
    n_dlu = seed_dlu_metadata(cursor)
    print(f"  {n_dlu} rows inserted into [DLU].[datalakeuniverse]")

    cursor.close()
    conn.close()
    print("Seeding complete.")


if __name__ == "__main__":
    main()

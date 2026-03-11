"""
Phase 1.3 — Seed Database Script
==================================
Reads data/seed/master_pii.csv and data/seed/dlu_metadata.csv and inserts
records into a local SQL Server instance.

  Tables created if they do not exist:
    [PII].[master_pii]            — 10 customer PII records
    [DLU].[datalakeuniverse]      — breach-file metadata rows

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
import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
SEED_DIR = PROJECT_ROOT / "data" / "seed"
MASTER_PII_CSV = SEED_DIR / "master_pii.csv"
DLU_METADATA_CSV = SEED_DIR / "dlu_metadata.csv"

# ---------------------------------------------------------------------------
# Database connection helpers
# ---------------------------------------------------------------------------

def _build_connection_string() -> str:
    server = os.getenv("DB_SERVER", "localhost")
    database = os.getenv("DB_NAME", "BreachSearch")
    user = os.getenv("DB_USER", "")
    password = os.getenv("DB_PASSWORD", "")

    driver = os.getenv("DB_DRIVER", "SQL Server")

    if user and password:
        return (
            f"DRIVER={{{driver}}};"
            f"SERVER={server};DATABASE={database};"
            f"UID={user};PWD={password}"
        )
    # Windows integrated authentication
    return (
        f"DRIVER={{{driver}}};"
        f"SERVER={server};DATABASE={database};"
        f"Trusted_Connection=yes"
    )


def get_connection():
    """Return a live pyodbc connection."""
    import pyodbc  # imported lazily so the module is importable without pyodbc installed
    return pyodbc.connect(_build_connection_string())


# ---------------------------------------------------------------------------
# Schema + table creation
# ---------------------------------------------------------------------------

DDL_STATEMENTS = [
    # Schemas
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'PII') EXEC('CREATE SCHEMA [PII]')",
    "IF NOT EXISTS (SELECT 1 FROM sys.schemas WHERE name = 'DLU') EXEC('CREATE SCHEMA [DLU]')",

    # PII.master_pii
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'PII' AND t.name = 'master_pii'
    )
    CREATE TABLE [PII].[master_pii] (
        ID              NVARCHAR(10)   NOT NULL PRIMARY KEY,
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

    # DLU.datalakeuniverse
    """
    IF NOT EXISTS (
        SELECT 1 FROM sys.tables t
        JOIN sys.schemas s ON t.schema_id = s.schema_id
        WHERE s.name = 'DLU' AND t.name = 'datalakeuniverse'
    )
    CREATE TABLE [DLU].[datalakeuniverse] (
        GUID            NVARCHAR(36)   NOT NULL PRIMARY KEY,
        MD5             NVARCHAR(32)   NULL,
        caseName        NVARCHAR(100)  NULL,
        fileName        NVARCHAR(255)  NULL,
        fileExtension   NVARCHAR(10)   NULL,
        TEXTPATH        NVARCHAR(500)  NULL,
        isExclusion     TINYINT        NULL DEFAULT 0
    )
    """,
]


def create_schemas_and_tables(cursor) -> None:
    for stmt in DDL_STATEMENTS:
        cursor.execute(stmt)
    cursor.connection.commit()
    print("  Schemas and tables verified/created.")


# ---------------------------------------------------------------------------
# Seed helpers
# ---------------------------------------------------------------------------

def seed_master_pii(cursor) -> int:
    """Insert rows from master_pii.csv; skip duplicates by primary key."""
    sql = """
    IF NOT EXISTS (SELECT 1 FROM [PII].[master_pii] WHERE ID = ?)
    INSERT INTO [PII].[master_pii]
        (ID, Fullname, FirstName, LastName, DOB, SSN, DriversLicense,
         Address1, Address2, Address3, ZipCode, City, State, Country)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    inserted = 0
    with open(MASTER_PII_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                row["ID"],          # for the EXISTS check
                row["ID"], row["Fullname"], row["FirstName"], row["LastName"],
                row["DOB"], row["SSN"], row["DriversLicense"],
                row["Address1"], row["Address2"], row["Address3"],
                row["ZipCode"], row["City"], row["State"], row["Country"],
            ))
            inserted += cursor.rowcount
    cursor.connection.commit()
    return inserted


def seed_dlu_metadata(cursor) -> int:
    """Insert rows from dlu_metadata.csv; skip duplicates by GUID."""
    sql = """
    IF NOT EXISTS (SELECT 1 FROM [DLU].[datalakeuniverse] WHERE GUID = ?)
    INSERT INTO [DLU].[datalakeuniverse]
        (GUID, MD5, caseName, fileName, fileExtension, TEXTPATH, isExclusion)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """
    inserted = 0
    with open(DLU_METADATA_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            cursor.execute(sql, (
                row["GUID"],        # for the EXISTS check
                row["GUID"], row["MD5"], row["caseName"],
                row["fileName"], row["fileExtension"], row["TEXTPATH"],
                int(row["isExclusion"]),
            ))
            inserted += cursor.rowcount
    cursor.connection.commit()
    return inserted


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    print("Connecting to SQL Server…")
    conn = get_connection()
    cursor = conn.cursor()

    print("Creating schemas and tables if needed…")
    create_schemas_and_tables(cursor)

    print("Seeding [PII].[master_pii]…")
    n_pii = seed_master_pii(cursor)
    print(f"  {n_pii} rows inserted into [PII].[master_pii]")

    print("Seeding [DLU].[datalakeuniverse]…")
    n_dlu = seed_dlu_metadata(cursor)
    print(f"  {n_dlu} rows inserted into [DLU].[datalakeuniverse]")

    cursor.close()
    conn.close()
    print("Seeding complete.")


if __name__ == "__main__":
    main()

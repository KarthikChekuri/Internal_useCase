"""
Tests for Phase V2-1.4: V2 Simulated Data and Seed Script.

TDD Red phase — these tests define the expected V2 behavior of:
  - scripts/generate_simulated_data.py  (rewritten for V2)
  - scripts/seed_database.py            (rewritten for V2)

Key V2 differences from V1:
  - Customer CSV is now `master_data.csv` (not master_pii.csv)
  - Primary key column is `customer_id` (INT, e.g. 1..10), not string "C001"
  - DLU metadata CSV has ONLY two columns: MD5 and file_path (no GUID, caseName, etc.)
  - file_path uses forward-slash convention: data/TEXT/{md5[:3]}/{md5}.ext
  - Seed script targets [PII].[master_data] and updated [DLU].[datalakeuniverse]

Each test maps to one or more spec scenarios from:
  openspec/changes/breach-pii-search/specs/simulated-data/spec.md
"""

import csv
import hashlib
import os
import re
import subprocess
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch, call

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SEED_DIR = DATA_DIR / "seed"
SIM_FILES_DIR = DATA_DIR / "simulated_files"
TEXT_DIR = DATA_DIR / "TEXT"
MASTER_DATA_CSV = SEED_DIR / "master_data.csv"
DLU_METADATA_CSV = SEED_DIR / "dlu_metadata.csv"
GENERATOR_SCRIPT = PROJECT_ROOT / "scripts" / "generate_simulated_data.py"
SEED_SCRIPT = PROJECT_ROOT / "scripts" / "seed_database.py"

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def generated_data():
    """Run the V2 generator script once for the whole module and return paths."""
    assert GENERATOR_SCRIPT.exists(), (
        f"Generator script not found at {GENERATOR_SCRIPT}"
    )
    result = subprocess.run(
        [sys.executable, str(GENERATOR_SCRIPT)],
        capture_output=True,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    if result.returncode != 0:
        pytest.fail(
            f"Generator script failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
    return {
        "master_data": MASTER_DATA_CSV,
        "dlu_metadata": DLU_METADATA_CSV,
        "sim_files_dir": SIM_FILES_DIR,
        "text_dir": TEXT_DIR,
    }


@pytest.fixture(scope="module")
def master_data_rows(generated_data):
    """Parsed rows from master_data.csv (list of dicts)."""
    with open(generated_data["master_data"], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@pytest.fixture(scope="module")
def dlu_rows(generated_data):
    """Parsed rows from dlu_metadata.csv (list of dicts)."""
    with open(generated_data["dlu_metadata"], newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


@pytest.fixture(scope="module")
def sim_file_paths(generated_data):
    """All files under data/simulated_files/."""
    return list(generated_data["sim_files_dir"].iterdir())


# ---------------------------------------------------------------------------
# master_data.csv tests (V2 schema)
# ---------------------------------------------------------------------------

# V2 has customer_id (INT PK) plus 13 PII fields = 14 columns total
MASTER_DATA_COLUMNS = [
    "customer_id",
    "Fullname", "FirstName", "LastName", "DOB", "SSN",
    "DriversLicense", "Address1", "Address2", "Address3",
    "ZipCode", "City", "State", "Country",
]


def test_master_data_csv_exists(generated_data):
    """Scenario: master_data.csv is created (V2 file name)."""
    assert generated_data["master_data"].exists(), (
        f"Expected master_data.csv at {generated_data['master_data']}"
    )


def test_master_data_has_ten_rows(master_data_rows):
    """Scenario: Generate customer records — exactly 10 rows."""
    assert len(master_data_rows) == 10, (
        f"Expected 10 customer rows, got {len(master_data_rows)}"
    )


def test_master_data_has_correct_columns(master_data_rows):
    """V2 master_data.csv must have customer_id as first column plus 13 PII fields."""
    actual_cols = list(master_data_rows[0].keys())
    assert actual_cols == MASTER_DATA_COLUMNS, (
        f"Column mismatch.\nExpected: {MASTER_DATA_COLUMNS}\nActual:   {actual_cols}"
    )


def test_master_data_customer_id_is_integer(master_data_rows):
    """Scenario: customer_id is an integer (1..10), not a string like 'C001'."""
    for row in master_data_rows:
        cid = row["customer_id"]
        assert cid.isdigit(), (
            f"customer_id '{cid}' is not an integer"
        )
        assert 1 <= int(cid) <= 10, (
            f"customer_id '{cid}' out of expected range 1-10"
        )


def test_master_data_customer_ids_are_unique(master_data_rows):
    """customer_id values must be unique (it is the PK)."""
    ids = [row["customer_id"] for row in master_data_rows]
    assert len(ids) == len(set(ids)), f"Duplicate customer_id values found: {ids}"


def test_master_data_name_diversity_apostrophe(master_data_rows):
    """Scenario: Customer name diversity — at least one name with apostrophe."""
    fullnames = [row["Fullname"] for row in master_data_rows]
    has_apostrophe = any("'" in name for name in fullnames)
    assert has_apostrophe, f"No name with apostrophe found in: {fullnames}"


def test_master_data_name_diversity_hyphen(master_data_rows):
    """Scenario: Customer name diversity — at least one hyphenated name."""
    fullnames = [row["Fullname"] for row in master_data_rows]
    has_hyphen = any("-" in name for name in fullnames)
    assert has_hyphen, f"No hyphenated name found in: {fullnames}"


def test_master_data_name_diversity_non_western(master_data_rows):
    """Scenario: Customer name diversity — at least one non-Western name."""
    non_western_markers = {"Patel", "Hassan", "Chekuri", "Rodriguez", "Kim", "Singh", "Chen"}
    fullnames = " ".join(row["Fullname"] for row in master_data_rows)
    has_non_western = any(marker in fullnames for marker in non_western_markers)
    assert has_non_western, (
        f"No non-Western name found. Names: {[r['Fullname'] for r in master_data_rows]}"
    )


def test_master_data_ssn_format(master_data_rows):
    """SSNs in master_data should follow XXX-XX-XXXX format."""
    ssn_pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")
    for row in master_data_rows:
        assert ssn_pattern.match(row["SSN"]), (
            f"SSN '{row['SSN']}' for '{row['Fullname']}' does not match XXX-XX-XXXX"
        )


def test_master_data_some_empty_address2(master_data_rows):
    """Some customers should have empty Address2 (realistic data)."""
    address2_values = [row["Address2"] for row in master_data_rows]
    has_empty = any(v == "" or v is None for v in address2_values)
    assert has_empty, "Expected at least one customer with empty Address2"


def test_master_data_some_empty_country(master_data_rows):
    """Some customers should have empty Country (realistic data)."""
    country_values = [row["Country"] for row in master_data_rows]
    has_empty = any(v == "" or v is None for v in country_values)
    assert has_empty, "Expected at least one customer with empty Country"


# ---------------------------------------------------------------------------
# dlu_metadata.csv tests (V2 simplified: only MD5 + file_path)
# ---------------------------------------------------------------------------

DLU_V2_COLUMNS = ["MD5", "file_path"]


def test_dlu_metadata_csv_exists(generated_data):
    """Scenario: dlu_metadata.csv is created."""
    assert generated_data["dlu_metadata"].exists(), (
        f"Expected dlu_metadata.csv at {generated_data['dlu_metadata']}"
    )


def test_dlu_metadata_has_only_two_columns(dlu_rows):
    """V2: dlu_metadata.csv must have ONLY MD5 and file_path columns."""
    actual_cols = list(dlu_rows[0].keys())
    assert actual_cols == DLU_V2_COLUMNS, (
        f"V2 DLU metadata must have only ['MD5', 'file_path'].\n"
        f"Expected: {DLU_V2_COLUMNS}\nActual:   {actual_cols}"
    )


def test_dlu_metadata_has_no_v1_columns(dlu_rows):
    """V2: No GUID, caseName, fileName, fileExtension, isExclusion columns."""
    v1_only_cols = {"GUID", "caseName", "fileName", "fileExtension", "isExclusion"}
    actual_cols = set(dlu_rows[0].keys())
    leftover = v1_only_cols & actual_cols
    assert not leftover, (
        f"V1 columns still present in V2 dlu_metadata.csv: {leftover}"
    )


def test_dlu_metadata_approx_25_rows(dlu_rows):
    """Scenario: Generate breach files — approximately 25 rows in dlu_metadata."""
    assert 20 <= len(dlu_rows) <= 35, (
        f"Expected ~25 rows in dlu_metadata.csv, got {len(dlu_rows)}"
    )


def test_dlu_metadata_file_path_convention(dlu_rows):
    """Scenario: file_path follows data/TEXT/{md5[:3]}/{md5}.ext convention."""
    file_path_pattern = re.compile(r"^data/TEXT/[0-9a-f]{3}/[0-9a-f]{32}\.\w+$")
    for row in dlu_rows:
        assert file_path_pattern.match(row["file_path"]), (
            f"file_path '{row['file_path']}' does not match "
            r"data/TEXT/{md5[:3]}/{md5}.ext pattern"
        )


def test_dlu_metadata_file_path_md5_consistency(dlu_rows):
    """MD5 prefix in file_path must match the MD5 column."""
    for row in dlu_rows:
        md5 = row["MD5"]
        file_path = row["file_path"]
        # e.g. data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt
        assert md5[:3] in file_path, (
            f"MD5 prefix '{md5[:3]}' not in file_path '{file_path}'"
        )
        assert md5 in file_path, (
            f"MD5 '{md5}' not found in file_path '{file_path}'"
        )


def test_dlu_metadata_md5_values_are_valid_hex(dlu_rows):
    """MD5 column must contain valid 32-character hex strings."""
    md5_pattern = re.compile(r"^[0-9a-f]{32}$")
    for row in dlu_rows:
        assert md5_pattern.match(row["MD5"]), (
            f"MD5 '{row['MD5']}' is not a valid 32-char hex string"
        )


# ---------------------------------------------------------------------------
# Dual-write and file existence tests
# ---------------------------------------------------------------------------

def test_simulated_files_directory_has_files(sim_file_paths):
    """Scenario: ~25 files exist in data/simulated_files/."""
    assert len(sim_file_paths) >= 20, (
        f"Expected at least 20 files in simulated_files/, found {len(sim_file_paths)}"
    )


def test_text_directory_exists(generated_data):
    """data/TEXT/ directory must exist after generation."""
    assert generated_data["text_dir"].exists(), "data/TEXT/ directory does not exist"


def test_text_directory_has_subdirectories(generated_data):
    """Scenario: TEXTPATH directory structure created automatically."""
    subdirs = [p for p in generated_data["text_dir"].iterdir() if p.is_dir()]
    assert len(subdirs) >= 1, "No subdirectories found under data/TEXT/"
    for d in subdirs:
        assert len(d.name) == 3 and all(c in "0123456789abcdef" for c in d.name), (
            f"Subdirectory '{d.name}' is not a 3-char hex prefix"
        )


def test_at_least_four_file_formats(sim_file_paths):
    """Scenario: At least 4 file formats (.txt, .xlsx, .csv, .xls) represented."""
    extensions = {p.suffix.lower() for p in sim_file_paths}
    required = {".txt", ".xlsx", ".csv", ".xls"}
    missing = required - extensions
    assert not missing, (
        f"Missing file format(s): {missing}. Found formats: {extensions}"
    )


def test_files_written_to_both_locations(dlu_rows, generated_data):
    """Scenario: File written to both locations with identical content."""
    failures = []
    for row in dlu_rows:
        file_path_str = row["file_path"]  # e.g. data/TEXT/c85/...txt
        # Construct absolute path from project root
        text_file = PROJECT_ROOT / Path(file_path_str.replace("/", os.sep))
        if not text_file.exists():
            failures.append(f"TEXT path file not found: {text_file}")

    assert not failures, "TEXT path files missing:\n" + "\n".join(failures)


def test_dual_write_files_have_identical_content(dlu_rows):
    """Scenario: Both copies contain identical content."""
    failures = []
    for row in dlu_rows:
        md5 = row["MD5"]
        file_path_str = row["file_path"]
        text_file = PROJECT_ROOT / Path(file_path_str.replace("/", os.sep))

        if not text_file.exists():
            failures.append(f"TEXT file not found: {text_file}")
            continue

        with open(text_file, "rb") as f:
            content = f.read()
        actual_md5 = hashlib.md5(content).hexdigest()
        if actual_md5 != md5:
            failures.append(
                f"MD5 mismatch for {text_file.name}: "
                f"expected={md5}, actual={actual_md5}"
            )
    assert not failures, "MD5 mismatches:\n" + "\n".join(failures)


def test_simulated_files_match_text_files(dlu_rows):
    """Files in simulated_files/ must match their TEXT counterparts byte-for-byte."""
    failures = []
    for row in dlu_rows:
        file_path_str = row["file_path"]
        text_file = PROJECT_ROOT / Path(file_path_str.replace("/", os.sep))
        # Find matching sim file by MD5
        md5 = row["MD5"]
        sim_files_for_md5 = list(SIM_FILES_DIR.glob("*"))
        matched_sim = None
        for sf in sim_files_for_md5:
            try:
                with open(sf, "rb") as f:
                    if hashlib.md5(f.read()).hexdigest() == md5:
                        matched_sim = sf
                        break
            except Exception:
                pass
        if matched_sim is None:
            failures.append(f"No simulated_file with MD5={md5} found")
            continue
        if not text_file.exists():
            failures.append(f"TEXT file not found: {text_file}")
            continue
        with open(matched_sim, "rb") as f:
            sim_content = f.read()
        with open(text_file, "rb") as f:
            text_content = f.read()
        if sim_content != text_content:
            failures.append(f"Content mismatch: {matched_sim.name} vs {text_file.name}")
    assert not failures, "File content mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# PII and intentional variation tests
# ---------------------------------------------------------------------------

def test_pii_from_master_appears_in_generated_files(master_data_rows, sim_file_paths):
    """Scenario: PII from master_data.csv appears in generated files."""
    text_content_all = []
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            try:
                text_content_all.append(p.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                pass

    combined = "\n".join(text_content_all)
    found = 0
    for row in master_data_rows:
        last_name = row["LastName"]
        if last_name and last_name in combined:
            found += 1

    assert found >= 8, (
        f"Only {found}/10 customer last names found in generated text files."
    )


def test_ssn_undashed_variant_present(sim_file_paths):
    """Scenario: SSN without dashes — some files use undashed SSN format."""
    undashed_ssn = re.compile(r"\b\d{9}\b")
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if undashed_ssn.search(content):
                found = True
                break
    assert found, "No file with undashed SSN (9 consecutive digits) found"


def test_dashed_ssn_variant_present(sim_file_paths):
    """Scenario: SSN with dashes — some files use dashed SSN format."""
    dashed_ssn = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if dashed_ssn.search(content):
                found = True
                break
    assert found, "No file with dashed SSN (XXX-XX-XXXX) found"


def test_name_misspelling_present(sim_file_paths):
    """Scenario: Name misspelling — 'Rodgriguez' appears for fuzzy matching test."""
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if "Rodgriguez" in content:
                found = True
                break
    assert found, "No file with misspelling 'Rodgriguez' found"


def test_name_last_first_reordering_present(sim_file_paths):
    """Scenario: Name reordering — 'Chekuri, Karthik' (last-first) appears."""
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if "Chekuri, Karthik" in content:
                found = True
                break
    assert found, "No file with 'Chekuri, Karthik' (last-first reordering) found"


def test_date_format_iso_present(sim_file_paths):
    """Date format variation: ISO (YYYY-MM-DD) present in at least one file."""
    iso_date = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if iso_date.search(content):
                found = True
                break
    assert found, "No ISO date format (YYYY-MM-DD) found in generated files"


def test_date_format_us_present(sim_file_paths):
    """Date format variation: US (MM/DD/YYYY) present in at least one file."""
    us_date = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if us_date.search(content):
                found = True
                break
    assert found, "No US date format (MM/DD/YYYY) found in generated files"


# ---------------------------------------------------------------------------
# Seed script unit tests (no real DB — all DB calls mocked)
# ---------------------------------------------------------------------------

def test_seed_script_exists():
    """seed_database.py script must exist."""
    assert SEED_SCRIPT.exists(), f"seed_database.py not found at {SEED_SCRIPT}"


def _load_seed_module():
    """Load seed_database.py as a module without triggering pyodbc at import time."""
    import importlib.util
    spec = importlib.util.spec_from_file_location("seed_database_v2", str(SEED_SCRIPT))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_seed_master_data_calls_correct_table(generated_data):
    """
    Scenario: Seed master data table — inserts into [PII].[master_data].
    Mock pyodbc so no real DB connection is made.
    """
    seed_mod = _load_seed_module()

    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_cursor.connection = MagicMock()

    # Run seed_master_data with the mock cursor
    n = seed_mod.seed_master_data(mock_cursor)

    # Verify it called cursor.execute (at least 10 times for 10 rows)
    assert mock_cursor.execute.call_count >= 10, (
        f"Expected at least 10 execute calls, got {mock_cursor.execute.call_count}"
    )

    # Verify the SQL references "PII"."master_data" (PostgreSQL quoting)
    calls_sql = [str(call_args) for call_args in mock_cursor.execute.call_args_list]
    assert any('"PII"."master_data"' in s for s in calls_sql), (
        'seed_master_data SQL does not reference "PII"."master_data"'
    )


def test_seed_dlu_calls_correct_table(generated_data):
    """
    Scenario: Seed DLU table — inserts into [DLU].[datalakeuniverse] with MD5 PK.
    Mock pyodbc so no real DB connection is made.
    """
    seed_mod = _load_seed_module()

    mock_cursor = MagicMock()
    mock_cursor.rowcount = 1
    mock_cursor.connection = MagicMock()

    n = seed_mod.seed_dlu_metadata(mock_cursor)

    calls_sql = [str(call_args) for call_args in mock_cursor.execute.call_args_list]
    assert any('"DLU"."datalakeuniverse"' in s for s in calls_sql), (
        'seed_dlu_metadata SQL does not reference "DLU"."datalakeuniverse"'
    )
    # V2: MD5 is the PK (not GUID)
    assert any("MD5" in s for s in calls_sql), (
        "seed_dlu_metadata SQL does not use MD5 as the lookup key"
    )


def test_seed_creates_schemas_and_tables():
    """
    Scenario: Seed script creates [PII] and [DLU] schemas if they don't exist.
    """
    seed_mod = _load_seed_module()

    mock_cursor = MagicMock()
    mock_conn = MagicMock()
    mock_cursor.connection = mock_conn

    seed_mod.create_schemas_and_tables(mock_cursor)

    # At least a few DDL calls should have been made (2 schemas + 2 tables = 4 statements)
    assert mock_cursor.execute.call_count >= 2, (
        f"Expected at least 2 DDL execute calls, got {mock_cursor.execute.call_count}"
    )
    all_sql = " ".join(str(c) for c in mock_cursor.execute.call_args_list)
    assert "PII" in all_sql, "DDL does not reference PII schema"
    assert "DLU" in all_sql, "DDL does not reference DLU schema"

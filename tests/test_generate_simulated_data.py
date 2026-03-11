"""
Tests for Phase 1.3: Simulated Data Generation.

TDD Red phase — these tests define the expected behavior of
scripts/generate_simulated_data.py before the script exists.

Each test maps to one or more spec scenarios from:
openspec/changes/breach-pii-search/specs/simulated-data/spec.md
"""

import csv
import hashlib
import os
import subprocess
import sys
from pathlib import Path

import pytest

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
SEED_DIR = DATA_DIR / "seed"
SIM_FILES_DIR = DATA_DIR / "simulated_files"
TEXT_DIR = DATA_DIR / "TEXT"
MASTER_PII_CSV = SEED_DIR / "master_pii.csv"
DLU_METADATA_CSV = SEED_DIR / "dlu_metadata.csv"
GENERATOR_SCRIPT = PROJECT_ROOT / "scripts" / "generate_simulated_data.py"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def generated_data():
    """Run the generator script once for the whole module and return paths."""
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
        "master_pii": MASTER_PII_CSV,
        "dlu_metadata": DLU_METADATA_CSV,
        "sim_files_dir": SIM_FILES_DIR,
        "text_dir": TEXT_DIR,
    }


@pytest.fixture(scope="module")
def master_pii_rows(generated_data):
    """Parsed rows from master_pii.csv (list of dicts)."""
    with open(generated_data["master_pii"], newline="", encoding="utf-8") as f:
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
# master_pii.csv tests
# ---------------------------------------------------------------------------

MASTER_PII_COLUMNS = [
    "ID", "Fullname", "FirstName", "LastName", "DOB", "SSN",
    "DriversLicense", "Address1", "Address2", "Address3",
    "ZipCode", "City", "State", "Country",
]


def test_master_pii_csv_exists(generated_data):
    """Scenario: master_pii.csv is created."""
    assert generated_data["master_pii"].exists(), (
        f"Expected {generated_data['master_pii']} to exist after generation"
    )


def test_master_pii_has_ten_rows(master_pii_rows):
    """Scenario: Generate customer records — exactly 10 rows."""
    assert len(master_pii_rows) == 10, (
        f"Expected 10 customer rows, got {len(master_pii_rows)}"
    )


def test_master_pii_has_correct_columns(master_pii_rows):
    """Scenario: All 13 PII fields present as CSV headers."""
    actual_cols = list(master_pii_rows[0].keys())
    assert actual_cols == MASTER_PII_COLUMNS, (
        f"Column mismatch.\nExpected: {MASTER_PII_COLUMNS}\nActual:   {actual_cols}"
    )


def test_master_pii_name_diversity_apostrophe(master_pii_rows):
    """Scenario: Customer name diversity — at least one name with apostrophe."""
    fullnames = [row["Fullname"] for row in master_pii_rows]
    has_apostrophe = any("'" in name for name in fullnames)
    assert has_apostrophe, (
        f"No name with apostrophe found in: {fullnames}"
    )


def test_master_pii_name_diversity_hyphen(master_pii_rows):
    """Scenario: Customer name diversity — at least one hyphenated name."""
    fullnames = [row["Fullname"] for row in master_pii_rows]
    has_hyphen = any("-" in name for name in fullnames)
    assert has_hyphen, (
        f"No hyphenated name found in: {fullnames}"
    )


def test_master_pii_name_diversity_non_western(master_pii_rows):
    """Scenario: Customer name diversity — at least one non-Western name."""
    non_western_markers = {"Patel", "Hassan", "Chekuri", "Rodriguez", "Kim", "Singh", "Chen"}
    fullnames = " ".join(row["Fullname"] for row in master_pii_rows)
    has_non_western = any(marker in fullnames for marker in non_western_markers)
    assert has_non_western, (
        f"No non-Western name found. Names: {[r['Fullname'] for r in master_pii_rows]}"
    )


def test_master_pii_ssn_format(master_pii_rows):
    """SSNs in master_pii should follow XXX-XX-XXXX format."""
    import re
    ssn_pattern = re.compile(r"^\d{3}-\d{2}-\d{4}$")
    for row in master_pii_rows:
        assert ssn_pattern.match(row["SSN"]), (
            f"SSN '{row['SSN']}' for '{row['Fullname']}' does not match XXX-XX-XXXX"
        )


def test_master_pii_some_null_address2(master_pii_rows):
    """Scenario: Some customers have null/empty Address2 fields."""
    address2_values = [row["Address2"] for row in master_pii_rows]
    has_empty = any(v == "" or v is None for v in address2_values)
    assert has_empty, "Expected at least one customer with empty Address2"


def test_master_pii_some_null_address3(master_pii_rows):
    """Scenario: Some customers have null/empty Address3 fields."""
    address3_values = [row["Address3"] for row in master_pii_rows]
    has_empty = any(v == "" or v is None for v in address3_values)
    assert has_empty, "Expected at least one customer with empty Address3"


def test_master_pii_some_null_country(master_pii_rows):
    """Scenario: Some customers have null/empty Country fields."""
    country_values = [row["Country"] for row in master_pii_rows]
    has_empty = any(v == "" or v is None for v in country_values)
    assert has_empty, "Expected at least one customer with empty Country"


# ---------------------------------------------------------------------------
# dlu_metadata.csv tests
# ---------------------------------------------------------------------------

DLU_COLUMNS = [
    "GUID", "MD5", "caseName", "fileName", "fileExtension", "TEXTPATH", "isExclusion",
]


def test_dlu_metadata_csv_exists(generated_data):
    """Scenario: dlu_metadata.csv is created."""
    assert generated_data["dlu_metadata"].exists(), (
        f"Expected {generated_data['dlu_metadata']} to exist"
    )


def test_dlu_metadata_approx_25_rows(dlu_rows):
    """Scenario: Generate breach files — approximately 25 rows in dlu_metadata."""
    assert 20 <= len(dlu_rows) <= 30, (
        f"Expected ~25 rows in dlu_metadata.csv, got {len(dlu_rows)}"
    )


def test_dlu_metadata_has_correct_columns(dlu_rows):
    """DLU metadata CSV must have exactly the expected columns."""
    actual_cols = list(dlu_rows[0].keys())
    assert actual_cols == DLU_COLUMNS, (
        f"Column mismatch.\nExpected: {DLU_COLUMNS}\nActual:   {actual_cols}"
    )


def test_dlu_metadata_case_name_consistent(dlu_rows):
    """All DLU rows should share the same caseName."""
    case_names = {row["caseName"] for row in dlu_rows}
    assert len(case_names) == 1, f"Multiple caseNames found: {case_names}"
    assert "Simulated" in list(case_names)[0] or "simulated" in list(case_names)[0].lower(), (
        f"caseName '{list(case_names)[0]}' does not contain 'Simulated'"
    )


def test_dlu_metadata_is_exclusion_zero(dlu_rows):
    """All rows must have isExclusion = 0."""
    for row in dlu_rows:
        assert row["isExclusion"] == "0", (
            f"Expected isExclusion=0 for file '{row['fileName']}', got '{row['isExclusion']}'"
        )


def test_dlu_metadata_textpath_convention(dlu_rows):
    """Scenario: TEXTPATH follows TEXT\\{md5[:3]}\\{md5}.{ext} convention."""
    import re
    textpath_pattern = re.compile(r"^TEXT\\[0-9a-f]{3}\\[0-9a-f]{32}\.\w+$")
    for row in dlu_rows:
        assert textpath_pattern.match(row["TEXTPATH"]), (
            f"TEXTPATH '{row['TEXTPATH']}' for '{row['fileName']}' does not match convention"
        )


def test_dlu_metadata_textpath_md5_consistency(dlu_rows):
    """TEXTPATH must embed the same MD5 as the MD5 column."""
    for row in dlu_rows:
        md5 = row["MD5"]
        textpath = row["TEXTPATH"]
        # TEXT\{md5[:3]}\{md5}.ext
        assert md5[:3] in textpath, (
            f"MD5 prefix '{md5[:3]}' not in TEXTPATH '{textpath}'"
        )
        assert md5 in textpath, (
            f"MD5 '{md5}' not found in TEXTPATH '{textpath}'"
        )


def test_dlu_metadata_guid_unique(dlu_rows):
    """Each row should have a unique GUID."""
    guids = [row["GUID"] for row in dlu_rows]
    assert len(guids) == len(set(guids)), "Duplicate GUIDs found in dlu_metadata"


# ---------------------------------------------------------------------------
# File existence tests
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
    """data/TEXT/ must have MD5-prefix subdirectories."""
    subdirs = [p for p in generated_data["text_dir"].iterdir() if p.is_dir()]
    assert len(subdirs) >= 1, "No subdirectories found under data/TEXT/"
    # Each subdir name should be exactly 3 hex characters
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


# ---------------------------------------------------------------------------
# MD5 integrity tests
# ---------------------------------------------------------------------------

def test_md5_matches_file_content_in_simulated_files(dlu_rows):
    """Scenario: MD5 in dlu_metadata matches actual file content hash."""
    failures = []
    for row in dlu_rows:
        filename = row["fileName"]
        expected_md5 = row["MD5"]
        file_path = SIM_FILES_DIR / filename
        if not file_path.exists():
            failures.append(f"File not found: {file_path}")
            continue
        with open(file_path, "rb") as f:
            actual_md5 = hashlib.md5(f.read()).hexdigest()
        if actual_md5 != expected_md5:
            failures.append(
                f"MD5 mismatch for '{filename}': "
                f"expected={expected_md5}, actual={actual_md5}"
            )
    assert not failures, "MD5 mismatches found:\n" + "\n".join(failures)


def test_text_path_files_exist_and_match_simulated(dlu_rows):
    """Scenario: File written to both locations with identical content."""
    failures = []
    for row in dlu_rows:
        textpath = row["TEXTPATH"]
        # TEXTPATH uses backslashes; convert for filesystem
        text_file = DATA_DIR / Path(textpath.replace("\\", os.sep))
        sim_file = SIM_FILES_DIR / row["fileName"]

        if not text_file.exists():
            failures.append(f"TEXT path file not found: {text_file}")
            continue
        if not sim_file.exists():
            failures.append(f"simulated_files file not found: {sim_file}")
            continue

        with open(sim_file, "rb") as f:
            sim_content = f.read()
        with open(text_file, "rb") as f:
            text_content = f.read()

        if sim_content != text_content:
            failures.append(
                f"Content mismatch between '{sim_file}' and '{text_file}'"
            )
    assert not failures, "File content mismatches:\n" + "\n".join(failures)


# ---------------------------------------------------------------------------
# PII appears in generated files
# ---------------------------------------------------------------------------

def test_pii_from_master_appears_in_generated_files(master_pii_rows, sim_file_paths):
    """Scenario: PII from master_pii.csv appears in generated files."""
    # Collect all text content from .txt and .csv files
    text_content_all = []
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            try:
                text_content_all.append(p.read_text(encoding="utf-8", errors="replace"))
            except Exception:
                pass

    combined = "\n".join(text_content_all)

    # At least 8 out of 10 customers should have a last name present somewhere
    found = 0
    for row in master_pii_rows:
        last_name = row["LastName"]
        if last_name and last_name in combined:
            found += 1

    assert found >= 8, (
        f"Only {found}/10 customer last names found in generated text files. "
        "PII may not be properly embedded."
    )


# ---------------------------------------------------------------------------
# Intentional PII variation tests
# ---------------------------------------------------------------------------

def test_ssn_undashed_variant_present(sim_file_paths):
    """Scenario: Some files use undashed SSN format (9 consecutive digits)."""
    import re
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
    """Scenario: Some files use dashed SSN format (XXX-XX-XXXX)."""
    import re
    dashed_ssn = re.compile(r"\b\d{3}-\d{2}-\d{4}\b")
    found = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if dashed_ssn.search(content):
                found = True
                break
    assert found, "No file with dashed SSN (XXX-XX-XXXX) found"


def test_name_misspelling_or_variation_present(sim_file_paths):
    """Scenario: At least one file contains a name misspelling or abbreviation variation."""
    # Look for known intentional misspellings/abbreviations from the spec
    variations = [
        "Rodgriguez",  # misspelling of Rodriguez
        "O'Brien",     # apostrophe name (also a variation marker)
        "Smith-Jones", # hyphenated
    ]
    # Also check for last-first ordering pattern like "Chekuri, Karthik"
    import re
    last_first_pattern = re.compile(r"\b\w+,\s+\w+\b")

    found_variation = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if any(v in content for v in variations):
                found_variation = True
                break
            if last_first_pattern.search(content):
                found_variation = True
                break

    assert found_variation, (
        "No name misspelling, abbreviation, or last-first ordering found in generated files"
    )


def test_date_format_variation_present(sim_file_paths):
    """Scenario: Multiple date formats used across files (ISO, US, European)."""
    import re
    # ISO: YYYY-MM-DD
    iso_date = re.compile(r"\b\d{4}-\d{2}-\d{2}\b")
    # US: MM/DD/YYYY
    us_date = re.compile(r"\b\d{1,2}/\d{1,2}/\d{4}\b")

    found_iso = False
    found_us = False
    for p in sim_file_paths:
        if p.suffix.lower() in (".txt", ".csv"):
            content = p.read_text(encoding="utf-8", errors="replace")
            if iso_date.search(content):
                found_iso = True
            if us_date.search(content):
                found_us = True

    assert found_iso or found_us, (
        "No date format variations (ISO or US) found in generated files"
    )

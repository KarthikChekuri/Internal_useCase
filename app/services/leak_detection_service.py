"""Leak Detection Engine — Phase 3.2 (V2 Adaptation).

Provides `detect_leaks(file_text, customer_pii) -> LeakDetectionResult`
that evaluates all 13 PII fields through a three-tier cascade:

  Tier 1: Exact regex (SSN, DOB, ZipCode, DriversLicense, State)
  Tier 2: Normalized substring (Fullname, FirstName, LastName, City,
           Address1-3, Country)
  Tier 3: Fuzzy sliding-window (Fullname, FirstName, LastName ONLY)

State is Tier 1 only (too short for substring). Non-name fields stop at
Tier 2. Disambiguation applies when FirstName found but Fullname and
LastName not found.

V2 change: detect_leaks() accepts MasterData (V2 table) instead of
MasterPII (V1). The function accepts any object with the 13 PII field
attributes (duck-typed via Any). DOB patterns now include European dot
format (DD.MM.YYYY) in addition to ISO, US, and European slash.
"""

import datetime
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from app.models.pii import FieldMatchResult
from app.utils.fuzzy import normalize_name, sliding_window_fuzzy

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# The 13 PII field names in evaluation order
ALL_FIELDS = [
    "SSN", "DOB", "DriversLicense", "ZipCode", "State",
    "Fullname", "FirstName", "LastName",
    "Address1", "Address2", "Address3",
    "City", "Country",
]

# Fields that get Tier 1 exact regex
TIER1_FIELDS = {"SSN", "DOB", "DriversLicense", "ZipCode", "State"}

# Fields that get Tier 2 normalized substring (State excluded)
TIER2_FIELDS = {
    "Fullname", "FirstName", "LastName",
    "City", "Address1", "Address2", "Address3", "Country",
}

# Fields that get Tier 3 fuzzy matching (name fields ONLY)
TIER3_FIELDS = {"Fullname", "FirstName", "LastName"}

SNIPPET_HALF = 50  # ~100 chars total context


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------

@dataclass
class LeakDetectionResult:
    """Per-field detection results for all 13 PII fields."""

    Fullname: FieldMatchResult = field(default_factory=lambda: _no_match())
    FirstName: FieldMatchResult = field(default_factory=lambda: _no_match())
    LastName: FieldMatchResult = field(default_factory=lambda: _no_match())
    DOB: FieldMatchResult = field(default_factory=lambda: _no_match())
    SSN: FieldMatchResult = field(default_factory=lambda: _no_match())
    DriversLicense: FieldMatchResult = field(default_factory=lambda: _no_match())
    Address1: FieldMatchResult = field(default_factory=lambda: _no_match())
    Address2: FieldMatchResult = field(default_factory=lambda: _no_match())
    Address3: FieldMatchResult = field(default_factory=lambda: _no_match())
    ZipCode: FieldMatchResult = field(default_factory=lambda: _no_match())
    City: FieldMatchResult = field(default_factory=lambda: _no_match())
    State: FieldMatchResult = field(default_factory=lambda: _no_match())
    Country: FieldMatchResult = field(default_factory=lambda: _no_match())
    needs_review: bool = False


def _no_match() -> FieldMatchResult:
    """Default not-found result."""
    return FieldMatchResult(found=False, method="none", confidence=0.0, snippet=None)


# ---------------------------------------------------------------------------
# Snippet extraction
# ---------------------------------------------------------------------------

def _extract_snippet(text: str, start: int, length: int) -> str:
    """Extract ~100 characters of context centered on the match.

    Args:
        text: Full file text.
        start: Character position where the match begins.
        length: Length of the matched string.

    Returns:
        A string of roughly 100 characters centered on the match.
    """
    match_center = start + length // 2
    snippet_start = max(0, match_center - SNIPPET_HALF)
    snippet_end = min(len(text), match_center + SNIPPET_HALF)
    return text[snippet_start:snippet_end]


# ---------------------------------------------------------------------------
# Tier 1: Exact regex matching
# ---------------------------------------------------------------------------

def _get_ssn_digits(ssn: str) -> str:
    """Strip dashes from SSN to get 9 digits."""
    return re.sub(r"[^0-9]", "", ssn)


def _tier1_ssn(text: str, ssn: str) -> Optional[FieldMatchResult]:
    """Check for exact SSN match (dashed and undashed), then partial last-4.

    Returns FieldMatchResult if any match, None otherwise.
    """
    digits = _get_ssn_digits(ssn)
    if len(digits) != 9:
        return None

    # Build dashed pattern: 343-43-4343
    dashed = f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"

    # Try full SSN match — dashed
    pattern_dashed = re.compile(re.escape(dashed))
    m = pattern_dashed.search(text)
    if m:
        snippet = _extract_snippet(text, m.start(), len(m.group()))
        return FieldMatchResult(found=True, method="exact", confidence=1.0, snippet=snippet)

    # Try full SSN match — undashed (9 consecutive digits)
    pattern_undashed = re.compile(r"\b" + re.escape(digits) + r"\b")
    m = pattern_undashed.search(text)
    if m:
        snippet = _extract_snippet(text, m.start(), len(m.group()))
        return FieldMatchResult(found=True, method="exact", confidence=1.0, snippet=snippet)

    # Try last-4 partial match with word boundary
    last4 = digits[-4:]
    pattern_last4 = re.compile(r"\b" + re.escape(last4) + r"\b")
    m = pattern_last4.search(text)
    if m:
        snippet = _extract_snippet(text, m.start(), len(m.group()))
        return FieldMatchResult(found=True, method="partial", confidence=0.40, snippet=snippet)

    return None


def _generate_dob_patterns(dob: datetime.date) -> list[str]:
    """Generate all date format representations of a DOB.

    Returns:
        List of date strings in ISO, US, European slash, and European dot
        formats. Duplicates are removed (preserving order).
    """
    patterns = []

    # ISO: YYYY-MM-DD
    patterns.append(dob.strftime("%Y-%m-%d"))

    # US: MM/DD/YYYY
    patterns.append(dob.strftime("%m/%d/%Y"))

    # European slash: DD/MM/YYYY
    patterns.append(dob.strftime("%d/%m/%Y"))

    # European dot: DD.MM.YYYY
    patterns.append(dob.strftime("%d.%m.%Y"))

    # Deduplicate (e.g., when day == month the US and European are different
    # but ISO is unique; when day > 12 the European format is unambiguous)
    return list(dict.fromkeys(patterns))


def _tier1_dob(text: str, dob: datetime.date) -> Optional[FieldMatchResult]:
    """Check for exact DOB match in any format."""
    patterns = _generate_dob_patterns(dob)
    for date_str in patterns:
        pattern = re.compile(re.escape(date_str))
        m = pattern.search(text)
        if m:
            snippet = _extract_snippet(text, m.start(), len(m.group()))
            return FieldMatchResult(found=True, method="exact", confidence=1.0, snippet=snippet)
    return None


def _tier1_word_boundary(text: str, value: str) -> Optional[FieldMatchResult]:
    """Check for exact match with word boundaries (State, ZipCode, DriversLicense)."""
    if not value:
        return None
    pattern = re.compile(r"\b" + re.escape(value) + r"\b")
    m = pattern.search(text)
    if m:
        snippet = _extract_snippet(text, m.start(), len(m.group()))
        return FieldMatchResult(found=True, method="exact", confidence=1.0, snippet=snippet)
    return None


def _tier1(text: str, field_name: str, value: Any) -> Optional[FieldMatchResult]:
    """Dispatch Tier 1 matching for the appropriate field."""
    if field_name == "SSN":
        return _tier1_ssn(text, value)
    elif field_name == "DOB":
        return _tier1_dob(text, value)
    elif field_name in ("ZipCode", "DriversLicense", "State"):
        return _tier1_word_boundary(text, str(value))
    return None


# ---------------------------------------------------------------------------
# Tier 2: Normalized substring matching
# ---------------------------------------------------------------------------

def _normalize_text(text: str) -> str:
    """Normalize text for Tier 2 comparison: lowercase, strip punctuation,
    collapse whitespace."""
    return normalize_name(text)


def _tier2(text: str, field_name: str, value: str, norm_text: str = "") -> Optional[FieldMatchResult]:
    """Normalized substring search.

    For Fullname: complete-string match (entire normalized name must appear
    as contiguous substring). Reordered names will NOT match.
    For other fields: simple substring.

    Args:
        text: Original file text (for snippet extraction).
        field_name: PII field name.
        value: PII value to search for.
        norm_text: Pre-normalized text (avoids re-normalizing per field).
    """
    if field_name == "State":
        # State excluded from Tier 2
        return None

    if not norm_text:
        norm_text = _normalize_text(text)
    norm_value = normalize_name(value)

    if not norm_value:
        return None

    # Find the normalized value as a contiguous substring in normalized text
    pos = norm_text.find(norm_value)
    if pos >= 0:
        # Map back to approximate position in original text for snippet
        # Since normalization can change positions, we do a best-effort match
        # in original text for snippet extraction
        snippet = _extract_snippet_from_normalized_match(text, value, pos)
        return FieldMatchResult(
            found=True, method="normalized", confidence=0.95, snippet=snippet
        )
    return None


def _extract_snippet_from_normalized_match(
    original_text: str, search_value: str, norm_pos: int
) -> str:
    """Extract snippet from original text near the normalized match position.

    Uses the normalized position as an approximate guide to find the right
    area in the original text.
    """
    # Best effort: use the normalized position as approximate position
    # The actual position in original text may differ slightly due to
    # punctuation removal / whitespace collapsing, but it's close enough
    # for a ~100 char snippet.
    approx_start = max(0, norm_pos)
    length = len(search_value)
    return _extract_snippet(original_text, approx_start, length)


# ---------------------------------------------------------------------------
# Tier 3: Fuzzy matching (name fields only)
# ---------------------------------------------------------------------------

def _tier3(text: str, field_name: str, value: str) -> Optional[FieldMatchResult]:
    """Fuzzy matching via sliding window — name fields only.

    Threshold: 75. Confidence = ratio / 100.
    """
    if field_name not in TIER3_FIELDS:
        return None

    score, position = sliding_window_fuzzy(text, value, threshold=75)
    if score >= 75.0:
        confidence = score / 100.0
        snippet = _extract_snippet(text, position, len(value))
        return FieldMatchResult(
            found=True, method="fuzzy", confidence=confidence, snippet=snippet
        )
    return None


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def detect_leaks(file_text: str, customer_pii: Any) -> LeakDetectionResult:
    """Detect PII leaks in file text by matching against a customer's known PII.

    Evaluates all 13 PII fields through a three-tier cascade:
      Tier 1 (exact regex) -> Tier 2 (normalized substring) -> Tier 3 (fuzzy)

    The first tier that matches wins for each field. State uses Tier 1 only.
    Non-name fields stop at Tier 2. Fuzzy (Tier 3) applies only to name fields.

    After per-field evaluation, the disambiguation rule is applied:
    if FirstName found but Fullname not found AND LastName not found,
    FirstName confidence is adjusted.

    Args:
        file_text: The full text content of the file to scan.
        customer_pii: MasterData instance (or any object) with attributes
                      for all 13 PII fields (Fullname, FirstName, LastName,
                      DOB, SSN, DriversLicense, Address1-3, ZipCode, City,
                      State, Country). V2 uses MasterData; V1 used MasterPII.

    Returns:
        LeakDetectionResult with per-field FieldMatchResult for all 13 fields.
    """
    result = LeakDetectionResult()
    needs_review = False

    # Normalize text once for all Tier 2 evaluations (avoids 8x re-normalization)
    norm_text = _normalize_text(file_text)

    # Evaluate each field through the cascade
    for field_name in ALL_FIELDS:
        raw_value = getattr(customer_pii, field_name, None)

        # Null/empty PII field handling
        if raw_value is None or (isinstance(raw_value, str) and raw_value.strip() == ""):
            setattr(result, field_name, _no_match())
            continue

        field_result = None

        # Tier 1: Exact regex
        if field_name in TIER1_FIELDS:
            field_result = _tier1(file_text, field_name, raw_value)

        # Tier 2: Normalized substring (if Tier 1 missed or not applicable)
        if field_result is None and field_name in TIER2_FIELDS:
            str_value = str(raw_value) if not isinstance(raw_value, str) else raw_value
            field_result = _tier2(file_text, field_name, str_value, norm_text=norm_text)

        # Tier 3: Fuzzy (if Tier 2 missed or not applicable, name fields only)
        if field_result is None and field_name in TIER3_FIELDS:
            str_value = str(raw_value) if not isinstance(raw_value, str) else raw_value
            field_result = _tier3(file_text, field_name, str_value)

        if field_result is not None:
            setattr(result, field_name, field_result)
        else:
            setattr(result, field_name, _no_match())

    # --- Disambiguation rule ---
    # Triggers ONLY when FirstName matches but Fullname.found == false
    # AND LastName.found == false.
    if (
        result.FirstName.found
        and not result.Fullname.found
        and not result.LastName.found
    ):
        if result.SSN.found:
            # SSN confirms identity -> confidence 0.70
            result.FirstName = FieldMatchResult(
                found=True,
                method=result.FirstName.method,
                confidence=0.70,
                snippet=result.FirstName.snippet,
            )
        else:
            # No SSN -> low confidence, needs review
            result.FirstName = FieldMatchResult(
                found=True,
                method=result.FirstName.method,
                confidence=0.40,
                snippet=result.FirstName.snippet,
            )
            needs_review = True

    result.needs_review = needs_review
    return result

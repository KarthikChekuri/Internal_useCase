"""Search Service V3 — Phase V3-2.2 + V3-3.1: Query Builder, Field Execution,
Result Merging, and Confidence Scoring.

Provides per-field query building and execution for the V3 Azure-only search
pipeline. Each PII field is searched individually with field-specific query
construction, search_mode, and metadata pre-filter. Results are merged
per-document and scored with a weighted confidence model.

Public API:
    build_field_query(field_name, field_value) -> str | None
    get_search_mode(field_name) -> str
    get_metadata_filter(field_name) -> str | None
    execute_field_query(search_client, field_name, field_value) -> list[tuple]
    merge_field_results(field_results) -> dict[str, dict]
    compute_confidence_v3(doc_fields, max_score) -> tuple[float, bool]
    search_customer_v3(search_client, customer) -> list[dict]

IMPORTANT: Do NOT import sqlalchemy or any DB models — it may hang.
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Field classification sets
# ---------------------------------------------------------------------------

# Name fields: tokenise on spaces, apply ~1 fuzzy to each token
_NAME_FIELDS = {"Fullname", "FirstName", "LastName"}

# Fields where the entire value is a quoted exact phrase (including multi-word)
_EXACT_PHRASE_FIELDS = {
    "ZipCode", "DriversLicense", "State", "City",
    "Address1", "Address2", "Address3", "Country",
}

# ---------------------------------------------------------------------------
# Search mode map
# ---------------------------------------------------------------------------

_SEARCH_MODE_MAP: dict[str, str] = {
    "SSN": "all",
    "DOB": "all",
    "Fullname": "any",
    "FirstName": "any",
    "LastName": "any",
    "ZipCode": "all",
    "DriversLicense": "all",
    "State": "all",
    "City": "all",
    "Address1": "all",
    "Address2": "all",
    "Address3": "all",
    "Country": "all",
}

# ---------------------------------------------------------------------------
# Metadata filter map
# ---------------------------------------------------------------------------

_METADATA_FILTER_MAP: dict[str, Optional[str]] = {
    "SSN": "has_ssn eq true",
    "Fullname": "has_name eq true",
    "FirstName": "has_name eq true",
    "LastName": "has_name eq true",
    "DOB": "has_dob eq true",
    "Address1": "has_address eq true",
    "Address2": "has_address eq true",
    "Address3": "has_address eq true",
    "City": None,
    "State": None,
    "ZipCode": None,
    "Country": None,
    "DriversLicense": None,
}

# When True, per-field queries include has_* pre-filters (requires PII-tagged index).
# Set to False when documents were indexed without Azure AI Language PII detection.
METADATA_FILTERS_ENABLED = False

# ---------------------------------------------------------------------------
# Lucene escaping for fuzzy name tokens
# ---------------------------------------------------------------------------

# Lucene special characters (apostrophe intentionally excluded — preserved as-is)
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}[\]^"~*?:\\/&|])')


def _escape_lucene_token(token: str) -> str:
    """Escape Lucene special characters in a single name token.

    Apostrophes are preserved (not a Lucene special character).
    All other Lucene special chars are backslash-escaped.
    """
    return _LUCENE_SPECIAL.sub(r"\\\1", token)


# ---------------------------------------------------------------------------
# Field-specific query formatters
# ---------------------------------------------------------------------------


def _format_ssn(value: str) -> str:
    """Build OR query for SSN: dashed form OR undashed form."""
    digits = re.sub(r"[^0-9]", "", value)
    if len(digits) == 9:
        dashed = f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
        return f'"{dashed}" OR "{digits}"'
    # Fallback: just quote what we have
    return f'"{value}"'


def _format_dob(value: str) -> str:
    """Build OR query for DOB from ISO string 'YYYY-MM-DD'.

    Produces four formats:
      US: MM/DD/YYYY
      ISO: YYYY-MM-DD
      European slash: DD/MM/YYYY
      European dot: DD.MM.YYYY
    """
    try:
        parsed = datetime.strptime(value.strip(), "%Y-%m-%d").date()
        us = f"{parsed.month:02d}/{parsed.day:02d}/{parsed.year:04d}"
        iso = f"{parsed.year:04d}-{parsed.month:02d}-{parsed.day:02d}"
        eu_slash = f"{parsed.day:02d}/{parsed.month:02d}/{parsed.year:04d}"
        eu_dot = f"{parsed.day:02d}.{parsed.month:02d}.{parsed.year:04d}"
        return f'"{us}" OR "{iso}" OR "{eu_slash}" OR "{eu_dot}"'
    except (ValueError, AttributeError):
        # Fallback: treat as opaque string
        return f'"{value}"'


def _format_name(value: str) -> str:
    """Build fuzzy query for name fields.

    Splits on whitespace, applies ~1 fuzzy operator to each token.
    Tokens are Lucene-escaped (apostrophes preserved).
    Returns space-separated fuzzy tokens (no OR — relies on search_mode=any).
    """
    tokens = [t.strip() for t in value.split() if t.strip()]
    if not tokens:
        return ""
    escaped = [_escape_lucene_token(t) for t in tokens]
    return " ".join(f"{t}~1" for t in escaped)


# ---------------------------------------------------------------------------
# Public: build_field_query
# ---------------------------------------------------------------------------


def build_field_query(field_name: str, field_value: Optional[str]) -> Optional[str]:
    """Build a Lucene query string for a single PII field value.

    Args:
        field_name: PII field name (e.g. "SSN", "Fullname", "DOB", "City").
        field_value: The customer's value for this field, or None/empty.

    Returns:
        Lucene query string ready for Azure AI Search, or None if value is
        null/empty (caller should skip this field).
    """
    # Guard: null or whitespace-only → skip
    if field_value is None:
        return None
    if not str(field_value).strip():
        return None

    value = str(field_value).strip()

    # SSN: dashed + undashed
    if field_name == "SSN":
        return _format_ssn(value)

    # DOB: four date format variants
    if field_name == "DOB":
        return _format_dob(value)

    # Name fields: tokenise + fuzzy each token
    if field_name in _NAME_FIELDS:
        query = _format_name(value)
        return query if query else None

    # Everything else: quoted exact phrase
    if field_name in _EXACT_PHRASE_FIELDS:
        return f'"{value}"'

    # Unknown field fallback: quoted exact
    logger.warning("build_field_query: unknown field '%s', using quoted exact.", field_name)
    return f'"{value}"'


# ---------------------------------------------------------------------------
# Public: get_search_mode
# ---------------------------------------------------------------------------


def get_search_mode(field_name: str) -> str:
    """Return the Azure AI Search search_mode for the given PII field.

    Args:
        field_name: PII field name.

    Returns:
        "any" for name fields (fuzzy recall), "all" for all others.
    """
    return _SEARCH_MODE_MAP.get(field_name, "all")


# ---------------------------------------------------------------------------
# Public: get_metadata_filter
# ---------------------------------------------------------------------------


def get_metadata_filter(field_name: str) -> Optional[str]:
    """Return the OData metadata pre-filter expression for the given PII field.

    When METADATA_FILTERS_ENABLED is False (documents indexed without PII
    detection), always returns None so queries search all documents.

    Args:
        field_name: PII field name.

    Returns:
        OData filter string (e.g. "has_ssn eq true"), or None if no pre-filter
        applies for this field or if filters are disabled.
    """
    if not METADATA_FILTERS_ENABLED:
        return None
    return _METADATA_FILTER_MAP.get(field_name, None)


# ---------------------------------------------------------------------------
# Public: execute_field_query
# ---------------------------------------------------------------------------


def execute_field_query(
    search_client,
    field_name: str,
    field_value: Optional[str],
) -> list[tuple[str, float, Optional[str]]]:
    """Execute a per-field Lucene query against Azure AI Search.

    Builds the query via build_field_query(), resolves the search_mode and
    metadata filter, then calls Azure AI Search with the fixed V3 parameters.

    Args:
        search_client: Azure SearchClient instance.
        field_name: PII field name (e.g. "SSN", "Fullname").
        field_value: Customer's value for this field, or None/empty.

    Returns:
        List of (md5, search_score, snippet_or_none) tuples.
        Returns empty list (no search call) if field_value is null/empty.
    """
    query = build_field_query(field_name, field_value)
    if query is None:
        return []

    search_mode = get_search_mode(field_name)
    metadata_filter = get_metadata_filter(field_name)

    raw_results = search_client.search(
        search_text=query,
        query_type="full",
        search_mode=search_mode,
        search_fields=["content", "content_phonetic", "content_lowercase"],
        scoring_profile="pii_boost",
        highlight_fields="content",
        highlight_pre_tag="[[MATCH]]",
        highlight_post_tag="[[/MATCH]]",
        filter=metadata_filter,
        top=100,
    )

    parsed: list[tuple[str, float, Optional[str]]] = []
    for result in raw_results:
        md5 = result["md5"]
        score = result["@search.score"]

        # Extract snippet from highlights
        highlights = result.get("@search.highlights")
        snippet: Optional[str] = None
        if highlights and "content" in highlights:
            content_hits = highlights["content"]
            if content_hits:
                snippet = content_hits[0]

        parsed.append((md5, score, snippet))

    return parsed


# ---------------------------------------------------------------------------
# Phase V3-3.1: Result merging and confidence scoring
# ---------------------------------------------------------------------------

# All 13 PII fields iterated during customer search
_PII_FIELDS = [
    "Fullname", "FirstName", "LastName",
    "DOB", "SSN", "DriversLicense",
    "Address1", "Address2", "Address3",
    "ZipCode", "City", "State", "Country",
]

# Name fields (used for name category confidence)
_CONF_NAME_FIELDS = {"Fullname", "FirstName", "LastName"}

# SSN field (used for SSN category confidence)
_CONF_SSN_FIELDS = {"SSN"}

# "Other" fields: all PII fields that are not SSN and not name fields
_CONF_OTHER_FIELDS = {
    "DOB", "DriversLicense",
    "Address1", "Address2", "Address3",
    "ZipCode", "City", "State", "Country",
}

# Confidence weights
_W_SSN = 0.35
_W_NAME = 0.30
_W_OTHER = 0.20
_W_DOC = 0.15  # document-level; always 0.0 in V3 (no broad query)


def merge_field_results(
    field_results: dict[str, list[tuple]],
) -> dict[str, dict]:
    """Merge per-field result lists into a single per-document dict.

    Args:
        field_results: Mapping of field_name -> list of (md5, score, snippet)
            tuples, as returned by execute_field_query per field.

    Returns:
        Dict keyed by md5, where each value is a dict of field_name ->
        {"found": True, "score": float, "snippet": str|None}.

    Example::

        merge_field_results({
            "SSN": [("md5_A", 12.5, "snip"), ("md5_B", 10.0, "snip2")],
            "Fullname": [("md5_A", 8.3, None), ("md5_C", 6.1, "snip3")],
        })
        # Returns:
        # {
        #     "md5_A": {
        #         "SSN": {"found": True, "score": 12.5, "snippet": "snip"},
        #         "Fullname": {"found": True, "score": 8.3, "snippet": None},
        #     },
        #     "md5_B": {"SSN": {"found": True, "score": 10.0, "snippet": "snip2"}},
        #     "md5_C": {"Fullname": {"found": True, "score": 6.1, "snippet": "snip3"}},
        # }
    """
    merged: dict[str, dict] = {}
    for field_name, tuples in field_results.items():
        for md5, score, snippet in tuples:
            if md5 not in merged:
                merged[md5] = {}
            merged[md5][field_name] = {
                "found": True,
                "score": score,
                "snippet": snippet,
            }
    return merged


def compute_confidence_v3(
    doc_fields: dict,
    max_score: float,
) -> tuple[float, bool]:
    """Compute a weighted confidence score and needs_review flag for one document.

    Per-field confidence is normalised as ``min(1.0, field_score / max_score)``.

    Weight categories:
    - SSN:   0.35
    - Name:  0.30  (max of Fullname, FirstName, LastName confidences)
    - Other: 0.20  (average of all found non-SSN/non-name field confidences)
    - Doc:   0.15  (always 0.0 in V3 — no broad query)

    Overall = 0.35*ssn_conf + 0.30*name_conf + 0.20*other_avg + 0.15*0.0

    needs_review is True when:
    - overall confidence < 0.5, OR
    - only FirstName matched (without Fullname, LastName, or SSN)

    Args:
        doc_fields: Per-field dict for one document, as produced by
            merge_field_results. Keys are field names; values are
            {"found": True, "score": float, "snippet": ...}.
        max_score: Highest raw score across ALL per-field queries for this
            customer (used as the normalisation denominator).

    Returns:
        (overall_confidence, needs_review) tuple.
    """

    def _field_conf(field_name: str) -> float:
        entry = doc_fields.get(field_name)
        if not entry or not entry.get("found"):
            return 0.0
        return min(1.0, entry.get("score", 0.0) / max_score)

    # SSN category
    ssn_conf = _field_conf("SSN")

    # Name category: max of the three name field confidences
    name_conf = max(
        _field_conf("Fullname"),
        _field_conf("FirstName"),
        _field_conf("LastName"),
    )

    # Other category: average of found "other" field confidences
    other_confs = [
        _field_conf(f)
        for f in _CONF_OTHER_FIELDS
        if f in doc_fields
    ]
    other_avg = (sum(other_confs) / len(other_confs)) if other_confs else 0.0

    overall = _W_SSN * ssn_conf + _W_NAME * name_conf + _W_OTHER * other_avg + _W_DOC * 0.0

    # needs_review rules
    firstname_only = (
        "FirstName" in doc_fields
        and "Fullname" not in doc_fields
        and "LastName" not in doc_fields
        and "SSN" not in doc_fields
    )
    needs_review = overall < 0.5 or firstname_only

    return float(overall), bool(needs_review)


def search_customer_v3(search_client, customer) -> list[dict]:
    """Search Azure AI Search for all non-null PII fields of a customer.

    Iterates the 13 known PII fields, skipping any that are None/empty.
    For each non-null field, calls execute_field_query and collects the
    (md5, score, snippet) tuples. Then merges all field results into a
    per-document structure and computes confidence per document.

    Args:
        search_client: Azure SearchClient instance.
        customer: An object with PII field attributes (e.g. SQLAlchemy
            MasterData model or a SimpleNamespace in tests).

    Returns:
        List of dicts, one per matched document, each containing:
            {
                "md5": str,
                "fields": dict,       # per-field results from merge_field_results
                "confidence": float,
                "needs_review": bool,
            }
        Returns empty list if no results are found across all fields.
    """
    field_results: dict[str, list[tuple]] = {}

    for field_name in _PII_FIELDS:
        field_value = getattr(customer, field_name, None)
        if field_value is None:
            continue
        # Also skip empty/whitespace strings
        if isinstance(field_value, str) and not field_value.strip():
            continue

        results = execute_field_query(search_client, field_name, field_value)
        if results:
            field_results[field_name] = results

    if not field_results:
        return []

    # Determine max_score across all fields for normalisation
    all_scores = [
        score
        for tuples in field_results.values()
        for (_md5, score, _snippet) in tuples
    ]
    max_score = max(all_scores) if all_scores else 1.0

    merged = merge_field_results(field_results)

    output = []
    for md5, doc_field_dict in merged.items():
        confidence, needs_review = compute_confidence_v3(doc_field_dict, max_score)
        output.append(
            {
                "md5": md5,
                "fields": doc_field_dict,
                "confidence": confidence,
                "needs_review": needs_review,
            }
        )

    return output


# ---------------------------------------------------------------------------
# Post-search enrichment: query all 13 fields for matched documents
# ---------------------------------------------------------------------------


def enrich_matched_documents(
    search_client,
    customer,
    matched_md5s: set[str],
) -> dict[str, dict]:
    """Query all 13 PII fields and return matches only for the given MD5s.

    This is the "post-search pass": after strategy filtering narrows down
    which documents are relevant, this function checks ALL PII fields
    against just those documents.  Each field fires one Azure query (top=100),
    and only hits for MD5s in ``matched_md5s`` are kept.

    Args:
        search_client: Azure SearchClient instance.
        customer: Customer object with PII attributes.
        matched_md5s: Set of MD5 strings that passed the strategy AND filter.

    Returns:
        Dict keyed by md5. Each value is a dict of field_name ->
        {"found": True, "score": float, "snippet": str|None},
        same shape as merge_field_results output.
    """
    if not matched_md5s:
        return {}

    field_results: dict[str, list[tuple]] = {}

    for field_name in _PII_FIELDS:
        field_value = getattr(customer, field_name, None)
        if field_value is None:
            continue
        if isinstance(field_value, str) and not field_value.strip():
            continue

        results = execute_field_query(search_client, field_name, field_value)
        if results:
            # Keep only results for docs that passed the strategy filter
            filtered = [
                (md5, score, snippet)
                for md5, score, snippet in results
                if md5 in matched_md5s
            ]
            if filtered:
                field_results[field_name] = filtered

    return merge_field_results(field_results)


# ---------------------------------------------------------------------------
# Strategy-driven search (V3 with strategies.yaml)
# ---------------------------------------------------------------------------


def search_customer_strategy_v3(
    search_client,
    customer,
    strategy_fields: list[str],
) -> list[dict]:
    """Search using a strategy's fields with AND logic.

    Sends per-field Lucene queries for ONLY the fields defined in the
    strategy.  Only returns documents where ALL non-null strategy fields
    were found (AND filter).  This replaces V2's Python leak detection
    with Azure AI Search per-field detection while keeping the same
    strategy-based candidate selection.

    Args:
        search_client: Azure SearchClient instance.
        customer: Customer object with PII attributes.
        strategy_fields: List of field names from the strategy
            (e.g. ``["Fullname", "SSN"]``).

    Returns:
        List of result dicts, one per document where ALL strategy fields
        matched.  Each dict contains:
            {
                "md5": str,
                "fields": dict,       # per-field match details
                "confidence": float,
                "needs_review": bool,
            }
        Returns empty list if no documents pass the AND filter.
    """
    field_results: dict[str, list[tuple]] = {}
    queried_fields: list[str] = []

    for field_name in strategy_fields:
        field_value = getattr(customer, field_name, None)
        if field_value is None:
            continue
        if isinstance(field_value, str) and not field_value.strip():
            continue

        queried_fields.append(field_name)
        results = execute_field_query(search_client, field_name, field_value)
        if results:
            field_results[field_name] = results

    # No queryable fields → skip
    if not queried_fields:
        return []

    # If any queried field returned zero results, AND filter means nothing matches
    if len(field_results) < len(queried_fields):
        return []

    # Merge results per document
    merged = merge_field_results(field_results)

    # AND filter: only keep documents where ALL queried fields were found
    filtered: dict[str, dict] = {}
    for md5, doc_fields in merged.items():
        if all(f in doc_fields for f in queried_fields):
            filtered[md5] = doc_fields

    if not filtered:
        return []

    # Compute max score across filtered results for normalisation
    all_scores = [
        entry["score"]
        for doc_fields in filtered.values()
        for entry in doc_fields.values()
        if entry.get("found") and entry.get("score") is not None
    ]
    max_score = max(all_scores) if all_scores else 1.0

    output = []
    for md5, doc_fields in filtered.items():
        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)
        output.append(
            {
                "md5": md5,
                "fields": doc_fields,
                "confidence": confidence,
                "needs_review": needs_review,
            }
        )

    return output

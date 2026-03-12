"""Search Service — Phase V2-2.2 (Strategy-Driven).

Provides strategy-driven search for the batch PII processing pipeline:

  1. Strategy definition: name, description, fields
  2. load_strategies(yaml_path): load + validate from YAML
  3. build_query_for_strategy(strategy, customer): Lucene query per strategy
  4. execute_strategy_query(search_client, query): Azure AI Search call
  5. search_customer(search_client, customer, strategies): multi-strategy union,
     dedup by MD5, highest score wins, strategy_that_found_it tracks first finder

V1 functions removed (replaced by batch service):
  - search_customer_pii, _lookup_customer, _validate_fullname, _build_lucene_query,
    _execute_search, _lookup_dlu_record, _resolve_file_path, _process_file,
    _persist_results
V1 exceptions removed: CustomerNotFoundError, DataIntegrityError, FullnameMismatchError
"""

import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid PII field names (from master_data table)
# ---------------------------------------------------------------------------

VALID_PII_FIELDS = {
    "Fullname", "FirstName", "LastName", "DOB", "SSN", "DriversLicense",
    "Address1", "Address2", "Address3", "ZipCode", "City", "State", "Country",
}

# Name fields: split into tokens, each gets ~1 fuzzy operator
_NAME_FIELDS = {"Fullname", "FirstName", "LastName"}

# City: also treated as tokens with ~1
_CITY_FIELDS = {"City"}

# Fields included as quoted exact strings
_QUOTED_FIELDS = {"DriversLicense", "Address1", "Address2", "Address3", "Country"}

# ---------------------------------------------------------------------------
# Strategy dataclass
# ---------------------------------------------------------------------------


@dataclass
class Strategy:
    """A search strategy defining which PII fields to use for Azure AI Search.

    Attributes:
        name: Unique identifier for this strategy (e.g., "fullname_ssn").
        description: Human-readable purpose description.
        fields: List of PII field names from master_data to use as search terms.
    """
    name: str
    description: str
    fields: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# YAML loading + validation
# ---------------------------------------------------------------------------


def load_strategies(yaml_path: str) -> list[Strategy]:
    """Load search strategies from a YAML configuration file.

    Reads the strategies.yaml file, validates field names, and returns
    a list of Strategy objects.

    Args:
        yaml_path: Path to the strategies YAML file.

    Returns:
        List of Strategy objects.

    Raises:
        FileNotFoundError: If the YAML file does not exist (message includes path).
        yaml.YAMLError: If the YAML file is malformed.
        ValueError: If a strategy references an invalid field name.
    """
    try:
        with open(yaml_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"strategies.yaml not found at: {yaml_path}"
        )
    except yaml.YAMLError as e:
        raise yaml.YAMLError(
            f"Invalid YAML in strategies.yaml ({yaml_path}): {e}"
        ) from e

    if data is None or "strategies" not in data:
        raise ValueError(
            f"strategies.yaml ({yaml_path}) must contain a 'strategies' key"
        )

    strategies = []
    for raw in data["strategies"]:
        name = raw.get("name", "")
        description = raw.get("description", "")
        fields = raw.get("fields", [])

        # Validate field names
        for f_name in fields:
            if f_name not in VALID_PII_FIELDS:
                valid_list = ", ".join(sorted(VALID_PII_FIELDS))
                raise ValueError(
                    f"Strategy '{name}' references invalid field '{f_name}'. "
                    f"Valid field names: {valid_list}"
                )

        strategies.append(Strategy(name=name, description=description, fields=fields))

    return strategies


# ---------------------------------------------------------------------------
# Lucene query helpers
# ---------------------------------------------------------------------------

# Lucene special characters that need escaping (excluding those we handle specially)
_LUCENE_SPECIAL = re.compile(r'([+\-!(){}[\]^"~*?:\\/])')


def _escape_lucene(token: str) -> str:
    """Escape Lucene special characters in a token.

    Removes apostrophes entirely and escapes other special chars.
    """
    token = token.replace("'", "")
    token = _LUCENE_SPECIAL.sub(r"\\\1", token)
    return token


def _tokenize_name(name: str) -> list[str]:
    """Tokenize a name for Lucene fuzzy query construction.

    Handles apostrophes (remove), hyphens (→ spaces), periods (remove).

    Args:
        name: Name string from customer record.

    Returns:
        List of cleaned name tokens.
    """
    if not name:
        return []
    name = name.replace("-", " ")
    name = name.replace("'", "")
    name = name.replace(".", "")
    return [t.strip() for t in name.split() if t.strip()]


def _format_ssn(ssn: str) -> list[str]:
    """Return SSN in both dashed and undashed quoted formats.

    Args:
        ssn: SSN string (dashed or undashed).

    Returns:
        List of quoted SSN variants, e.g. ['"343-43-4343"', '"343434343"'].
    """
    digits = re.sub(r"[^0-9]", "", ssn)
    if len(digits) != 9:
        return [f'"{ssn}"']
    dashed = f"{digits[0:3]}-{digits[3:5]}-{digits[5:9]}"
    return [f'"{dashed}"', f'"{digits}"']


def _format_dob(dob: Any) -> list[str]:
    """Return DOB in ISO, US, and European date formats.

    Args:
        dob: datetime.date object or string.

    Returns:
        List of quoted date strings in three formats.
    """
    try:
        year = dob.year
        month = dob.month
        day = dob.day
    except AttributeError:
        # Fallback: treat as string
        return [f'"{dob}"']

    iso = f"{year:04d}-{month:02d}-{day:02d}"
    us = f"{month:02d}/{day:02d}/{year:04d}"
    eu = f"{day:02d}/{month:02d}/{year:04d}"
    return [f'"{iso}"', f'"{us}"', f'"{eu}"']


def _build_field_terms(field_name: str, value: Any) -> list[str]:
    """Build Lucene term(s) for a single PII field value.

    Args:
        field_name: One of the VALID_PII_FIELDS.
        value: The customer's value for this field (may be None or empty).

    Returns:
        List of Lucene term strings. Empty list if value is null/empty.
    """
    if value is None:
        return []
    if isinstance(value, str) and not value.strip():
        return []

    if field_name in _NAME_FIELDS or field_name in _CITY_FIELDS:
        tokens = _tokenize_name(str(value))
        return [f"{_escape_lucene(t)}~1" for t in tokens]

    if field_name == "SSN":
        return _format_ssn(str(value))

    if field_name == "DOB":
        return _format_dob(value)

    if field_name in _QUOTED_FIELDS:
        return [f'"{value}"']

    if field_name == "ZipCode":
        return [str(value)]

    if field_name == "State":
        return [str(value)]

    # Fallback: treat as quoted exact string
    return [f'"{value}"']


# ---------------------------------------------------------------------------
# Public: build_query_for_strategy
# ---------------------------------------------------------------------------


def build_query_for_strategy(strategy: Strategy, customer: Any) -> Optional[str]:
    """Build a Lucene query for a customer using the given strategy's fields.

    Combines all non-null field values with OR logic into a single Lucene query.
    Field values are formatted according to their type (fuzzy names, SSN variants,
    DOB formats, quoted exact strings, etc.).

    Args:
        strategy: Strategy defining which PII fields to include.
        customer: Customer record (MasterData or any object with PII attributes).

    Returns:
        Lucene query string, or None if all strategy fields are null/empty
        for this customer (strategy is effectively skipped, logged as warning).
    """
    all_terms: list[str] = []

    for field_name in strategy.fields:
        value = getattr(customer, field_name, None)
        terms = _build_field_terms(field_name, value)
        all_terms.extend(terms)

    if not all_terms:
        logger.warning(
            "Strategy '%s' has no query terms for customer — all fields null. Skipping.",
            strategy.name,
        )
        return None

    return " OR ".join(all_terms)


# ---------------------------------------------------------------------------
# Public: execute_strategy_query
# ---------------------------------------------------------------------------


def execute_strategy_query(search_client: Any, query: str) -> list[dict]:
    """Execute a single strategy's Lucene query against Azure AI Search.

    Calls Azure AI Search with:
      - queryType="full" (enables Lucene syntax: fuzzy ~1, OR, quoted phrases)
      - searchMode="any" (maximizes recall — file matches if ANY term matches)
      - searchFields: content, content_phonetic, content_lowercase
      - scoringProfile: "pii_boost"
      - top: 100

    Args:
        search_client: Azure SearchClient instance.
        query: Full Lucene query string.

    Returns:
        List of result dicts with md5, file_path, and search_score.
    """
    raw_results = search_client.search(
        search_text=query,
        query_type="full",
        search_mode="any",
        search_fields=["content", "content_phonetic", "content_lowercase"],
        scoring_profile="pii_boost",
        top=100,
    )

    parsed: list[dict] = []
    for result in raw_results:
        parsed.append({
            "md5": result["md5"],
            "file_path": result["file_path"],
            "search_score": result["@search.score"],
        })

    return parsed


# ---------------------------------------------------------------------------
# Public: search_customer
# ---------------------------------------------------------------------------


def search_customer(
    search_client: Any,
    customer: Any,
    strategies: list[Strategy],
) -> list[dict]:
    """Run all strategies for a customer and return the union of candidate files.

    For each strategy:
      1. Build a Lucene query from strategy fields + customer PII values.
      2. Skip the strategy if the query is None (all fields null).
      3. Execute the query against Azure AI Search.
      4. Merge results into the candidate set (dedup by MD5).

    Deduplication rules:
      - Same MD5 returned by multiple strategies → one entry in the result.
      - strategy_that_found_it: the name of the FIRST strategy that found the file.
      - azure_search_score: the HIGHEST score across all strategies for that file.

    Args:
        search_client: Azure SearchClient instance.
        customer: Customer record (MasterData or any object with PII attributes).
        strategies: List of Strategy objects to run.

    Returns:
        List of candidate dicts, each with:
          - md5: str
          - file_path: str
          - azure_search_score: float (highest across strategies)
          - strategy_that_found_it: str (first strategy that found this file)
    """
    # candidates dict: md5 → candidate entry
    candidates: dict[str, dict] = {}

    for strategy in strategies:
        query = build_query_for_strategy(strategy, customer)

        if query is None:
            # All fields null for this strategy — skip (already logged as warning)
            continue

        results = execute_strategy_query(search_client, query)

        if not results:
            logger.info(
                "Strategy '%s' returned 0 results for customer %s.",
                strategy.name,
                getattr(customer, "customer_id", "unknown"),
            )
            continue

        logger.info(
            "Strategy '%s' returned %d results for customer %s.",
            strategy.name,
            len(results),
            getattr(customer, "customer_id", "unknown"),
        )

        for result in results:
            md5 = result["md5"]
            score = result["search_score"]

            if md5 not in candidates:
                # First time we see this file — record the strategy that found it
                candidates[md5] = {
                    "md5": md5,
                    "file_path": result["file_path"],
                    "azure_search_score": score,
                    "strategy_that_found_it": strategy.name,
                }
            else:
                # Already found — keep highest score; strategy_that_found_it stays as first finder
                if score > candidates[md5]["azure_search_score"]:
                    candidates[md5]["azure_search_score"] = score

    return list(candidates.values())

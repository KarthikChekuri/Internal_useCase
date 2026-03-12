"""
app/utils/strategy_loader.py — Load and validate strategies from a YAML file.

A strategy defines which PII fields to use as search terms when querying
Azure AI Search. Each strategy has a name, description, and list of field
names from the master_data table.
"""
import logging
from dataclasses import dataclass, field
from typing import List

import yaml

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Valid PII field names (columns from master_data / MasterPII table)
# ---------------------------------------------------------------------------

VALID_PII_FIELDS: frozenset = frozenset({
    "Fullname",
    "FirstName",
    "LastName",
    "DOB",
    "SSN",
    "DriversLicense",
    "Address1",
    "Address2",
    "Address3",
    "ZipCode",
    "City",
    "State",
    "Country",
})


# ---------------------------------------------------------------------------
# Strategy dataclass
# ---------------------------------------------------------------------------

@dataclass
class Strategy:
    """Represents a single search strategy.

    Attributes:
        name: Unique identifier for this strategy (e.g. "fullname_ssn").
        description: Human-readable description of the strategy's purpose.
        fields: List of PII field names to use when building the search query.
    """
    name: str
    description: str
    fields: List[str]


# ---------------------------------------------------------------------------
# Loader
# ---------------------------------------------------------------------------

def load_strategies(yaml_path: str) -> List[Strategy]:
    """Load and validate strategies from a YAML configuration file.

    Args:
        yaml_path: Absolute or relative path to the strategies YAML file.

    Returns:
        A list of validated Strategy objects.

    Raises:
        FileNotFoundError: If the file does not exist at yaml_path.
        ValueError: If the YAML is malformed, the 'strategies' key is missing,
                    the list is empty, or any strategy references an invalid
                    PII field name.
    """
    # --- File existence check ---
    import os
    if not os.path.exists(yaml_path):
        raise FileNotFoundError(
            f"strategies file not found: {yaml_path}"
        )

    # --- Parse YAML ---
    try:
        with open(yaml_path, "r", encoding="utf-8") as fh:
            data = yaml.safe_load(fh)
    except yaml.YAMLError as exc:
        raise ValueError(
            f"Failed to parse strategies YAML at {yaml_path}: {exc}"
        ) from exc

    # --- Structural validation ---
    if not isinstance(data, dict) or "strategies" not in data:
        raise ValueError(
            f"strategies file at {yaml_path} must contain a top-level "
            f"'strategies' key."
        )

    raw_strategies = data["strategies"]

    if not raw_strategies:
        raise ValueError(
            f"strategies list in {yaml_path} is empty. "
            f"Define at least one strategy."
        )

    # --- Build and validate Strategy objects ---
    strategies: List[Strategy] = []
    for item in raw_strategies:
        name = item.get("name", "")
        description = item.get("description", "")
        fields: List[str] = item.get("fields", [])

        # Validate each field name
        invalid_fields = [f for f in fields if f not in VALID_PII_FIELDS]
        if invalid_fields:
            valid_sorted = sorted(VALID_PII_FIELDS)
            raise ValueError(
                f"Strategy '{name}' references invalid PII field(s): "
                f"{invalid_fields}. "
                f"Valid field names are: {valid_sorted}"
            )

        strategies.append(Strategy(name=name, description=description, fields=fields))
        logger.debug("Loaded strategy '%s' with fields: %s", name, fields)

    logger.info("Loaded %d strategies from %s", len(strategies), yaml_path)
    return strategies

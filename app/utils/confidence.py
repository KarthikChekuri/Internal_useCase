"""Confidence scoring utilities for breach PII search (Phase 2.4).

Provides:
- normalize_search_scores: normalizes Azure AI Search scores to 0.0-1.0
- compute_per_field_confidence: maps detection methods to confidence values
- compute_overall_confidence: weighted formula selecting by anchor presence
"""

import logging

logger = logging.getLogger(__name__)


def normalize_search_scores(scores: list[float]) -> list[float]:
    """Normalize Azure AI Search scores to the 0.0-1.0 range.

    Divides each score by the maximum score in the result set so
    the highest-scoring file gets a normalized score of 1.0.

    Args:
        scores: Raw search scores from Azure AI Search.

    Returns:
        List of normalized scores in the same order, each in [0.0, 1.0].
    """
    if not scores:
        return []

    max_score = max(scores)
    if max_score == 0.0:
        return [0.0] * len(scores)

    return [s / max_score for s in scores]


def compute_per_field_confidence(method: str, ratio: float | None = None) -> float:
    """Compute confidence for a single PII field based on its detection method.

    Args:
        method: Detection method — one of "exact", "normalized", "fuzzy",
                "partial", "none", "first_name_with_ssn", "first_name_only".
        ratio: Required when method is "fuzzy". The rapidfuzz ratio (0-100).

    Returns:
        Confidence score between 0.0 and 1.0.

    Raises:
        ValueError: If method is unknown or fuzzy is used without a ratio.
    """
    method_map: dict[str, float] = {
        "exact": 1.0,
        "normalized": 0.95,
        "partial": 0.40,
        "none": 0.0,
        "first_name_with_ssn": 0.70,
        "first_name_only": 0.40,
    }

    if method == "fuzzy":
        if ratio is None:
            raise ValueError("'fuzzy' method requires a 'ratio' argument.")
        return ratio / 100.0

    if method not in method_map:
        raise ValueError(
            f"Unknown detection method '{method}'. "
            f"Valid methods: {sorted(method_map.keys())} or 'fuzzy'."
        )

    return method_map[method]


def compute_overall_confidence(
    ssn_conf: float,
    name_conf: float,
    other_field_confs: list[float],
    search_score_norm: float,
) -> dict:
    """Compute overall file confidence using weighted scenario formulas.

    Scenario selection:
    - ssn_conf > 0 AND name_conf > 0  -> SSN+Name formula
    - ssn_conf > 0 AND name_conf == 0 -> SSN-only formula
    - name_conf > 0 AND ssn_conf == 0 -> Name-only formula
    - both == 0                        -> No-anchor fallback (needs_review)

    Args:
        ssn_conf: Per-field confidence for SSN (0.0 if no match).
        name_conf: Max confidence among Fullname, FirstName, LastName
                   (0.0 if none match). Caller is responsible for computing
                   the max across name fields before calling this function.
        other_field_confs: Confidence scores for evaluable non-anchor fields.
                          Only include fields that are non-null in master_pii.
                          Unmatched but evaluable fields should be 0.0.
                          Null fields should be excluded entirely.
        search_score_norm: Normalized Azure AI Search score (0.0-1.0).

    Returns:
        Dict with keys:
          - score (float): overall confidence 0.0-1.0
          - scenario (str): "ssn_and_name", "ssn_only", "name_only", "no_anchor"
          - needs_review (bool): True for no-anchor fallback
          - other_fields_avg (float): computed average of other fields
    """
    # Compute OtherFields_avg
    if other_field_confs:
        other_fields_avg = sum(other_field_confs) / len(other_field_confs)
    else:
        other_fields_avg = 0.0

    # Scenario selection
    needs_review = False

    if ssn_conf > 0 and name_conf > 0:
        scenario = "ssn_and_name"
        score = (
            0.40 * ssn_conf
            + 0.30 * name_conf
            + 0.15 * other_fields_avg
            + 0.15 * search_score_norm
        )
    elif ssn_conf > 0 and name_conf == 0:
        scenario = "ssn_only"
        score = (
            0.60 * ssn_conf
            + 0.15 * other_fields_avg
            + 0.25 * search_score_norm
        )
    elif name_conf > 0 and ssn_conf == 0:
        scenario = "name_only"
        score = (
            0.50 * name_conf
            + 0.20 * other_fields_avg
            + 0.30 * search_score_norm
        )
    else:
        scenario = "no_anchor"
        score = 0.50 * other_fields_avg + 0.50 * search_score_norm
        needs_review = True

    # Clamp to [0.0, 1.0]
    score = max(0.0, min(1.0, score))

    return {
        "score": score,
        "scenario": scenario,
        "needs_review": needs_review,
        "other_fields_avg": other_fields_avg,
    }

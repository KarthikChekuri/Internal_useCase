"""V1 Search Service Tests — REPLACED by V2.

This file previously tested the V1 search_service.py (search_customer_pii,
_lookup_customer, _validate_fullname, _build_lucene_query, etc.).

V1 functions have been removed as part of Phase V2-2.2 (Strategy-Driven Search).
V1 exceptions (CustomerNotFoundError, DataIntegrityError, FullnameMismatchError)
have also been removed.

All search service tests are now in:
    tests/services/test_search_service_v2.py

Score normalization tests are kept here since normalize_search_scores is unchanged.
"""

import pytest

from app.utils.confidence import normalize_search_scores


class TestScoreNormalization:
    """Tests for search score normalization (unchanged from V1)."""

    def test_normalize_scores_divides_by_max(self):
        """Verify scores are normalized by dividing by max."""
        raw_scores = [10.0, 5.0, 2.5]
        normalized = normalize_search_scores(raw_scores)
        assert normalized == [1.0, 0.5, 0.25]

    def test_normalize_scores_empty_list(self):
        """Empty input returns empty output."""
        assert normalize_search_scores([]) == []

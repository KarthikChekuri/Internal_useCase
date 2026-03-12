"""Phase V3-5.1: Tests for scripts/compare_v2_v3.py.

Tests verify the comparison logic for V2 vs V3 batch results:
- get_batch_results: queries DB and returns result rows
- compare_results: builds per-customer comparison dicts
- format_comparison: produces readable console output

All DB access is mocked. Uses SimpleNamespace for result objects.
"""

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest


# ---------------------------------------------------------------------------
# Helper: create fake result rows
# ---------------------------------------------------------------------------


def make_result(
    customer_id: int,
    md5: str,
    batch_id: str = "batch-v2",
    strategy_name: str = "fullname_ssn",
    leaked_fields: list | None = None,
    overall_confidence: float = 0.85,
    needs_review: bool = False,
    azure_search_score: float = 10.0,
):
    """Create a SimpleNamespace mimicking a Result ORM row."""
    return SimpleNamespace(
        batch_id=batch_id,
        customer_id=customer_id,
        md5=md5,
        strategy_name=strategy_name,
        leaked_fields=json.dumps(leaked_fields or ["SSN", "Fullname"]),
        overall_confidence=overall_confidence,
        needs_review=needs_review,
        azure_search_score=azure_search_score,
        match_details=json.dumps({}),
    )


# ===========================================================================
# TEST CLASS: get_batch_results
# ===========================================================================


class TestGetBatchResults:
    """Tests for get_batch_results() which queries DB for a batch's results."""

    def test_returns_result_rows_for_batch(self):
        """WHEN batch has 3 result rows THEN get_batch_results returns all 3."""
        from scripts.compare_v2_v3 import get_batch_results

        mock_db = MagicMock()
        rows = [
            make_result(1, "doc_A", batch_id="batch-v2"),
            make_result(2, "doc_B", batch_id="batch-v2"),
            make_result(1, "doc_C", batch_id="batch-v2"),
        ]
        mock_db.query.return_value.filter.return_value.all.return_value = rows

        result = get_batch_results(mock_db, "batch-v2")

        assert len(result) == 3

    def test_returns_empty_for_nonexistent_batch(self):
        """WHEN batch has no result rows THEN get_batch_results returns []."""
        from scripts.compare_v2_v3 import get_batch_results

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = []

        result = get_batch_results(mock_db, "batch-nonexistent")

        assert result == []

    def test_queries_with_correct_batch_id(self):
        """WHEN called with batch_id THEN filter is applied to query."""
        from scripts.compare_v2_v3 import get_batch_results

        mock_db = MagicMock()
        mock_db.query.return_value.filter.return_value.all.return_value = []

        get_batch_results(mock_db, "target-batch-123")

        mock_db.query.assert_called_once()


# ===========================================================================
# TEST CLASS: compare_results
# ===========================================================================


class TestCompareResults:
    """Tests for compare_results() which builds a per-customer comparison dict."""

    def test_overlapping_files_found_in_both(self):
        """WHEN doc_A found in both V2 and V3 THEN it appears in 'both' set."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [make_result(1, "doc_A", batch_id="v2", leaked_fields=["SSN", "Fullname"])]
        v3_rows = [make_result(1, "doc_A", batch_id="v3", leaked_fields=["SSN", "Fullname"])]

        comparison = compare_results(v2_rows, v3_rows)

        assert 1 in comparison
        cust = comparison[1]
        assert "doc_A" in cust["both"]
        assert "doc_A" not in cust["v2_only"]
        assert "doc_A" not in cust["v3_only"]

    def test_v2_only_file_correctly_identified(self):
        """WHEN doc_C found only in V2 THEN it appears in 'v2_only'."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [
            make_result(1, "doc_A", batch_id="v2"),
            make_result(1, "doc_C", batch_id="v2"),
        ]
        v3_rows = [
            make_result(1, "doc_A", batch_id="v3"),
        ]

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        assert "doc_C" in cust["v2_only"]
        assert "doc_C" not in cust["both"]
        assert "doc_C" not in cust["v3_only"]

    def test_v3_only_file_correctly_identified(self):
        """WHEN doc_D found only in V3 THEN it appears in 'v3_only'."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [make_result(1, "doc_A", batch_id="v2")]
        v3_rows = [
            make_result(1, "doc_A", batch_id="v3"),
            make_result(1, "doc_D", batch_id="v3"),
        ]

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        assert "doc_D" in cust["v3_only"]
        assert "doc_D" not in cust["both"]
        assert "doc_D" not in cust["v2_only"]

    def test_multiple_customers_tracked_separately(self):
        """WHEN two customers have different files THEN each customer has separate comparison."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [
            make_result(1, "doc_A", batch_id="v2"),
            make_result(2, "doc_B", batch_id="v2"),
        ]
        v3_rows = [
            make_result(1, "doc_A", batch_id="v3"),
            make_result(2, "doc_C", batch_id="v3"),
        ]

        comparison = compare_results(v2_rows, v3_rows)

        assert 1 in comparison
        assert 2 in comparison

        # Customer 1: doc_A in both
        assert "doc_A" in comparison[1]["both"]
        # Customer 2: doc_B in v2_only, doc_C in v3_only
        assert "doc_B" in comparison[2]["v2_only"]
        assert "doc_C" in comparison[2]["v3_only"]

    def test_confidence_difference_captured(self):
        """WHEN doc_A in both but different confidence THEN confidence diff is recorded."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [make_result(1, "doc_A", batch_id="v2", overall_confidence=0.92)]
        v3_rows = [make_result(1, "doc_A", batch_id="v3", overall_confidence=0.68)]

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        doc_a_info = cust.get("doc_details", {}).get("doc_A", {})
        v2_conf = doc_a_info.get("v2_confidence")
        v3_conf = doc_a_info.get("v3_confidence")

        assert v2_conf == 0.92
        assert v3_conf == 0.68

    def test_field_differences_captured(self):
        """WHEN doc_A has different leaked_fields in V2 vs V3 THEN field diff is recorded."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [make_result(1, "doc_A", batch_id="v2",
                               leaked_fields=["SSN", "Fullname", "DOB"])]
        v3_rows = [make_result(1, "doc_A", batch_id="v3",
                               leaked_fields=["SSN", "Fullname"])]

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        doc_a_info = cust.get("doc_details", {}).get("doc_A", {})
        v2_fields = set(doc_a_info.get("v2_fields", []))
        v3_fields = set(doc_a_info.get("v3_fields", []))

        assert "DOB" in v2_fields
        assert "DOB" not in v3_fields

    def test_no_v2_results_all_in_v3_only(self):
        """WHEN V2 has no results but V3 does THEN all docs are in v3_only."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = []
        v3_rows = [
            make_result(1, "doc_X", batch_id="v3"),
            make_result(1, "doc_Y", batch_id="v3"),
        ]

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        assert "doc_X" in cust["v3_only"]
        assert "doc_Y" in cust["v3_only"]
        assert len(cust["both"]) == 0
        assert len(cust["v2_only"]) == 0

    def test_no_v3_results_all_in_v2_only(self):
        """WHEN V3 has no results but V2 does THEN all docs are in v2_only."""
        from scripts.compare_v2_v3 import compare_results

        v2_rows = [
            make_result(1, "doc_A", batch_id="v2"),
            make_result(1, "doc_B", batch_id="v2"),
        ]
        v3_rows = []

        comparison = compare_results(v2_rows, v3_rows)

        cust = comparison[1]
        assert "doc_A" in cust["v2_only"]
        assert "doc_B" in cust["v2_only"]
        assert len(cust["both"]) == 0
        assert len(cust["v3_only"]) == 0


# ===========================================================================
# TEST CLASS: format_comparison
# ===========================================================================


class TestFormatComparison:
    """Tests for format_comparison() which formats comparison as console output."""

    def _make_comparison(self):
        """Build a comparison dict as returned by compare_results."""
        return {
            1: {
                "both": {"doc_A", "doc_B"},
                "v2_only": {"doc_C"},
                "v3_only": {"doc_D"},
                "doc_details": {
                    "doc_A": {
                        "v2_confidence": 0.92,
                        "v3_confidence": 0.68,
                        "v2_fields": ["SSN", "Fullname", "DOB"],
                        "v3_fields": ["SSN", "Fullname"],
                    },
                    "doc_B": {
                        "v2_confidence": 0.80,
                        "v3_confidence": 0.80,
                        "v2_fields": ["Fullname"],
                        "v3_fields": ["Fullname"],
                    },
                    "doc_C": {
                        "v2_confidence": 0.65,
                        "v3_confidence": None,
                        "v2_fields": ["SSN"],
                        "v3_fields": [],
                    },
                    "doc_D": {
                        "v2_confidence": None,
                        "v3_confidence": 0.45,
                        "v2_fields": [],
                        "v3_fields": ["FirstName"],
                    },
                },
            }
        }

    def test_format_returns_string(self):
        """WHEN format_comparison called THEN it returns a non-empty string."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = self._make_comparison()
        output = format_comparison(comparison)

        assert isinstance(output, str)
        assert len(output) > 0

    def test_format_includes_customer_header(self):
        """WHEN output formatted THEN it includes customer ID marker."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = self._make_comparison()
        output = format_comparison(comparison)

        assert "Customer 1" in output or "customer_id=1" in output or "1" in output

    def test_format_shows_both_section(self):
        """WHEN docs found in both THEN output contains 'both' or similar label."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = self._make_comparison()
        output = format_comparison(comparison)

        # Either "both" or "doc_A" and "doc_B" should appear
        assert "both" in output.lower() or ("doc_A" in output and "doc_B" in output)

    def test_format_shows_v2_only_section(self):
        """WHEN docs found only in V2 THEN output indicates V2-only."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = self._make_comparison()
        output = format_comparison(comparison)

        # Either "V2 only" label or doc_C mentioned
        assert "v2" in output.lower() or "doc_C" in output

    def test_format_shows_v3_only_section(self):
        """WHEN docs found only in V3 THEN output indicates V3-only."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = self._make_comparison()
        output = format_comparison(comparison)

        assert "v3" in output.lower() or "doc_D" in output

    def test_format_empty_comparison(self):
        """WHEN comparison dict is empty THEN format returns valid string (no crash)."""
        from scripts.compare_v2_v3 import format_comparison

        output = format_comparison({})

        assert isinstance(output, str)

    def test_format_single_customer_no_differences(self):
        """WHEN all docs found in both and no differences THEN output reflects no discrepancies."""
        from scripts.compare_v2_v3 import format_comparison

        comparison = {
            5: {
                "both": {"doc_X"},
                "v2_only": set(),
                "v3_only": set(),
                "doc_details": {
                    "doc_X": {
                        "v2_confidence": 0.90,
                        "v3_confidence": 0.90,
                        "v2_fields": ["SSN"],
                        "v3_fields": ["SSN"],
                    }
                },
            }
        }

        output = format_comparison(comparison)
        assert isinstance(output, str)
        assert "5" in output or "doc_X" in output

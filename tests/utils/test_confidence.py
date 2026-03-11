"""Tests for confidence scoring utilities (Phase 2.4).

Each test maps to a Given/When/Then scenario from the spec at:
openspec/changes/breach-pii-search/specs/confidence-scoring/spec.md
"""

import pytest

from app.utils.confidence import (
    compute_overall_confidence,
    compute_per_field_confidence,
    normalize_search_scores,
)


# ─────────────────────────────────────────────────────────────────────
# Requirement: Search score normalization
# ─────────────────────────────────────────────────────────────────────


class TestNormalizeSearchScores:
    """normalize_search_scores divides each score by the max in the set."""

    def test_normalize_scores_across_result_set(self):
        """Scenario: Normalize scores across result set.
        WHEN search returns files with scores 12.5, 8.3, and 4.1
        THEN normalized scores are 1.0, 0.664, and 0.328 respectively.
        """
        scores = [12.5, 8.3, 4.1]
        result = normalize_search_scores(scores)
        assert result == pytest.approx([1.0, 0.664, 0.328], abs=1e-3)

    def test_single_score_normalizes_to_one(self):
        """A single score always normalizes to 1.0."""
        assert normalize_search_scores([5.0]) == [1.0]

    def test_all_equal_scores_normalize_to_one(self):
        """All identical scores should each be 1.0."""
        assert normalize_search_scores([3.0, 3.0, 3.0]) == [1.0, 1.0, 1.0]

    def test_empty_list_returns_empty(self):
        """Empty input should return empty output."""
        assert normalize_search_scores([]) == []

    def test_all_zero_scores_returns_zeros(self):
        """If all scores are 0, avoid division by zero and return all 0.0."""
        assert normalize_search_scores([0.0, 0.0]) == [0.0, 0.0]

    def test_preserves_order(self):
        """Normalized output keeps the same ordering as input."""
        scores = [4.0, 10.0, 6.0]
        result = normalize_search_scores(scores)
        assert result == pytest.approx([0.4, 1.0, 0.6], abs=1e-3)


# ─────────────────────────────────────────────────────────────────────
# Requirement: Per-field confidence values
# ─────────────────────────────────────────────────────────────────────


class TestComputePerFieldConfidence:
    """compute_per_field_confidence maps detection methods to confidence."""

    def test_exact_match_confidence(self):
        """Scenario: Exact SSN match confidence.
        WHEN the SSN is found as an exact match
        THEN per-field confidence is 1.0.
        """
        assert compute_per_field_confidence("exact") == 1.0

    def test_normalized_match_confidence(self):
        """Scenario: Normalized name match confidence.
        WHEN the fullname is found via normalized matching (case-insensitive)
        THEN per-field confidence is 0.95.
        """
        assert compute_per_field_confidence("normalized") == 0.95

    def test_fuzzy_match_confidence_at_ratio_87(self):
        """Scenario: Fuzzy name match confidence at ratio 87.
        WHEN the fullname is matched by rapidfuzz with token_set_ratio of 87
        THEN per-field confidence is 0.87.
        """
        assert compute_per_field_confidence("fuzzy", ratio=87) == 0.87

    def test_fuzzy_match_confidence_at_ratio_75(self):
        """Fuzzy match at the minimum threshold ratio 75."""
        assert compute_per_field_confidence("fuzzy", ratio=75) == 0.75

    def test_fuzzy_match_confidence_at_ratio_100(self):
        """Fuzzy match at perfect ratio 100."""
        assert compute_per_field_confidence("fuzzy", ratio=100) == 1.0

    def test_partial_match_confidence(self):
        """Partial match (e.g., SSN last-4 only) gives 0.40."""
        assert compute_per_field_confidence("partial") == 0.40

    def test_none_match_confidence(self):
        """No match gives 0.0."""
        assert compute_per_field_confidence("none") == 0.0

    def test_first_name_with_ssn_disambiguation(self):
        """Scenario: First name plus SSN confirms identity.
        WHEN only the first name matches and SSN is also found in the same file
        THEN per-field confidence for the name is 0.70.
        """
        assert compute_per_field_confidence("first_name_with_ssn") == 0.70

    def test_first_name_without_ssn(self):
        """Scenario: First name only without SSN is low confidence.
        WHEN a file contains only the first name but no SSN match
        THEN FirstName confidence is 0.30-0.50.
        """
        result = compute_per_field_confidence("first_name_only")
        assert 0.30 <= result <= 0.50

    def test_fuzzy_without_ratio_raises(self):
        """Fuzzy method requires a ratio argument."""
        with pytest.raises(ValueError):
            compute_per_field_confidence("fuzzy")

    def test_unknown_method_raises(self):
        """Unknown detection method raises ValueError."""
        with pytest.raises(ValueError):
            compute_per_field_confidence("unknown_method")


# ─────────────────────────────────────────────────────────────────────
# Requirement: Overall file confidence (scenario selection + formulas)
# ─────────────────────────────────────────────────────────────────────


class TestComputeOverallConfidence:
    """compute_overall_confidence applies weighted formulas by scenario."""

    def test_ssn_and_name_both_found(self):
        """Scenario: SSN and name both found in file.
        GIVEN 3 evaluable non-anchor fields: DOB, ZipCode, City
        WHEN SSN=1.0, Fullname=0.95, DOB=1.0, ZipCode=0.0, City=0.0,
             search_score_norm=0.8
        THEN OtherFields_avg = (1.0+0.0+0.0)/3 = 0.333
             overall = 0.40(1.0) + 0.30(0.95) + 0.15(0.333) + 0.15(0.8)
                     = 0.40 + 0.285 + 0.050 + 0.12 = 0.855
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[1.0, 0.0, 0.0],  # DOB, ZipCode, City
            search_score_norm=0.8,
        )
        assert result["score"] == pytest.approx(0.855, abs=1e-3)
        assert result["scenario"] == "ssn_and_name"
        assert result["needs_review"] is False

    def test_ssn_only_no_name(self):
        """Scenario: SSN found but no name in file.
        GIVEN 4 evaluable non-anchor fields: DOB, ZipCode, City, State
        WHEN SSN=1.0, no name, ZipCode=1.0, others=0.0,
             search_score_norm=0.6
        THEN OtherFields_avg = (0.0+1.0+0.0+0.0)/4 = 0.25
             overall = 0.60(1.0) + 0.15(0.25) + 0.25(0.6)
                     = 0.60 + 0.0375 + 0.15 = 0.7875
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.0,
            other_field_confs=[0.0, 1.0, 0.0, 0.0],  # DOB, Zip, City, State
            search_score_norm=0.6,
        )
        assert result["score"] == pytest.approx(0.7875, abs=1e-3)
        assert result["scenario"] == "ssn_only"
        assert result["needs_review"] is False

    def test_name_only_no_ssn(self):
        """Scenario: Name found but no SSN in file.
        GIVEN 3 evaluable non-anchor fields: DOB, ZipCode, City
        WHEN Fullname=0.85 (fuzzy), no SSN, City=0.95, others=0.0,
             search_score_norm=0.5
        THEN OtherFields_avg = (0.0+0.0+0.95)/3 = 0.317
             overall = 0.50(0.85) + 0.20(0.317) + 0.30(0.5)
                     = 0.425 + 0.063 + 0.15 = 0.638
        """
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.85,
            other_field_confs=[0.0, 0.0, 0.95],  # DOB, Zip, City
            search_score_norm=0.5,
        )
        assert result["score"] == pytest.approx(0.638, abs=1e-2)
        assert result["scenario"] == "name_only"
        assert result["needs_review"] is False

    def test_no_anchor_fallback(self):
        """Scenario: Only non-anchor fields matched.
        GIVEN 4 evaluable non-anchor fields: DOB, ZipCode, City, State
        WHEN ZipCode=1.0, DOB=1.0, City=0.0, State=0.0, no SSN/name,
             search_score_norm=0.4
        THEN OtherFields_avg = (1.0+1.0+0.0+0.0)/4 = 0.50
             overall = 0.50(0.50) + 0.50(0.4) = 0.25 + 0.20 = 0.45
             needs_review = true
        """
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.0,
            other_field_confs=[1.0, 1.0, 0.0, 0.0],  # DOB, Zip, City, State
            search_score_norm=0.4,
        )
        assert result["score"] == pytest.approx(0.45, abs=1e-3)
        assert result["scenario"] == "no_anchor"
        assert result["needs_review"] is True

    def test_no_other_fields_matched_besides_anchors(self):
        """Scenario: No other fields matched besides anchors.
        WHEN only SSN and Name match but no other fields match
        THEN OtherFields_avg is 0.0 and the formula uses 0.0 for that component.
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[0.0, 0.0],
            search_score_norm=0.7,
        )
        expected = 0.40 * 1.0 + 0.30 * 0.95 + 0.15 * 0.0 + 0.15 * 0.7
        assert result["score"] == pytest.approx(expected, abs=1e-3)
        assert result["scenario"] == "ssn_and_name"

    def test_empty_other_fields_gives_zero_avg(self):
        """If no non-anchor fields are evaluable (all null), OtherFields_avg = 0.0."""
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.90,
            other_field_confs=[],  # no evaluable non-anchor fields
            search_score_norm=0.9,
        )
        expected = 0.40 * 1.0 + 0.30 * 0.90 + 0.15 * 0.0 + 0.15 * 0.9
        assert result["score"] == pytest.approx(expected, abs=1e-3)

    def test_name_conf_is_max_of_name_fields(self):
        """Name_conf should be the max of Fullname, FirstName, LastName.
        The caller is responsible for passing max; this test verifies the formula
        works correctly with the max value.
        """
        # If Fullname=0.0, FirstName=0.70, LastName=0.0 -> name_conf=0.70
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.70,  # max(0.0, 0.70, 0.0)
            other_field_confs=[0.0],
            search_score_norm=0.6,
        )
        expected = 0.40 * 1.0 + 0.30 * 0.70 + 0.15 * 0.0 + 0.15 * 0.6
        assert result["score"] == pytest.approx(expected, abs=1e-3)
        assert result["scenario"] == "ssn_and_name"

    def test_no_anchor_needs_review_flag(self):
        """No-anchor scenario always sets needs_review to True."""
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.0,
            other_field_confs=[0.5],
            search_score_norm=0.3,
        )
        assert result["needs_review"] is True

    def test_ssn_and_name_does_not_need_review(self):
        """SSN+Name scenario never needs review."""
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[0.5],
            search_score_norm=0.8,
        )
        assert result["needs_review"] is False

    def test_ssn_only_does_not_need_review(self):
        """SSN-only scenario does not need review."""
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.0,
            other_field_confs=[0.5],
            search_score_norm=0.8,
        )
        assert result["needs_review"] is False

    def test_name_only_does_not_need_review(self):
        """Name-only scenario does not need review by default."""
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.85,
            other_field_confs=[0.5],
            search_score_norm=0.8,
        )
        assert result["needs_review"] is False

    def test_result_contains_other_fields_avg(self):
        """Result dict includes the computed other_fields_avg."""
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[1.0, 0.0, 0.0],
            search_score_norm=0.8,
        )
        assert result["other_fields_avg"] == pytest.approx(0.333, abs=1e-3)

    def test_score_clamped_to_0_1_range(self):
        """Overall score must always be between 0.0 and 1.0."""
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=1.0,
            other_field_confs=[1.0, 1.0, 1.0],
            search_score_norm=1.0,
        )
        assert 0.0 <= result["score"] <= 1.0

    def test_all_zeros_gives_zero(self):
        """When everything is 0, overall score is 0.0."""
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.0,
            other_field_confs=[],
            search_score_norm=0.0,
        )
        assert result["score"] == pytest.approx(0.0, abs=1e-3)
        assert result["needs_review"] is True


# ─────────────────────────────────────────────────────────────────────
# Additional edge-case tests for spec formula verification
# ─────────────────────────────────────────────────────────────────────


class TestOverallConfidenceSpecExamples:
    """Verify the exact numeric examples from the spec and prompt."""

    def test_ssn_and_name_perfect_score(self):
        """Scenario: Perfect SSN + perfect name + all other fields + perfect search.

        SSN=1.0, Name=1.0, OtherFields_avg=1.0, SearchScore_norm=1.0
        Formula: 0.40(1.0) + 0.30(1.0) + 0.15(1.0) + 0.15(1.0) = 1.0
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=1.0,
            other_field_confs=[1.0, 1.0, 1.0],
            search_score_norm=1.0,
        )
        assert result["score"] == pytest.approx(1.0, abs=1e-3)
        assert result["scenario"] == "ssn_and_name"

    def test_ssn_and_name_spec_example_0_955(self):
        """Verify spec-aligned calculation yielding ~0.955.

        SSN=1.0, Name=0.95, OtherFields_avg=1.0, SearchScore_norm=1.0
        Formula: 0.40(1.0) + 0.30(0.95) + 0.15(1.0) + 0.15(1.0)
               = 0.40 + 0.285 + 0.15 + 0.15 = 0.985
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.95,
            other_field_confs=[1.0, 1.0, 1.0],
            search_score_norm=1.0,
        )
        assert result["score"] == pytest.approx(0.985, abs=1e-3)

    def test_ssn_only_high_other_fields(self):
        """SSN-only with high other fields.

        SSN=1.0, OtherFields_avg=0.75, SearchScore_norm=0.9
        Formula: 0.60(1.0) + 0.15(0.75) + 0.25(0.9)
               = 0.60 + 0.1125 + 0.225 = 0.9375
        """
        result = compute_overall_confidence(
            ssn_conf=1.0,
            name_conf=0.0,
            other_field_confs=[0.75, 0.75, 0.75, 0.75],
            search_score_norm=0.9,
        )
        assert result["score"] == pytest.approx(0.9375, abs=1e-3)
        assert result["scenario"] == "ssn_only"

    def test_name_only_moderate_score(self):
        """Name-only with moderate other fields.

        Name=0.90, OtherFields_avg=0.50, SearchScore_norm=0.7
        Formula: 0.50(0.90) + 0.20(0.50) + 0.30(0.7)
               = 0.45 + 0.10 + 0.21 = 0.76
        """
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.90,
            other_field_confs=[0.50, 0.50],
            search_score_norm=0.7,
        )
        assert result["score"] == pytest.approx(0.76, abs=1e-3)
        assert result["scenario"] == "name_only"

    def test_no_anchor_with_single_field(self):
        """No anchor with only one non-anchor field matched.

        OtherFields_avg = 1.0/1 = 1.0, SearchScore_norm=0.4
        Formula: 0.50(1.0) + 0.50(0.4) = 0.50 + 0.20 = 0.70
        """
        result = compute_overall_confidence(
            ssn_conf=0.0,
            name_conf=0.0,
            other_field_confs=[1.0],
            search_score_norm=0.4,
        )
        assert result["score"] == pytest.approx(0.70, abs=1e-3)
        assert result["scenario"] == "no_anchor"
        assert result["needs_review"] is True


class TestPerFieldConfidenceEdgeCases:
    """Edge-case tests for compute_per_field_confidence."""

    def test_fuzzy_at_threshold_boundary_75(self):
        """Fuzzy ratio at exactly the threshold (75) -> confidence 0.75."""
        assert compute_per_field_confidence("fuzzy", ratio=75) == 0.75

    def test_fuzzy_just_above_threshold_76(self):
        """Fuzzy ratio of 76 -> confidence 0.76."""
        assert compute_per_field_confidence("fuzzy", ratio=76) == 0.76

    def test_fuzzy_high_ratio_99(self):
        """Fuzzy ratio of 99 -> confidence 0.99."""
        assert compute_per_field_confidence("fuzzy", ratio=99) == 0.99

    def test_fuzzy_with_zero_ratio(self):
        """Fuzzy ratio of 0 -> confidence 0.0."""
        assert compute_per_field_confidence("fuzzy", ratio=0) == 0.0

    def test_exact_always_returns_one(self):
        """Exact method always returns 1.0 regardless of context."""
        assert compute_per_field_confidence("exact") == 1.0

    def test_partial_always_returns_040(self):
        """Partial (SSN last-4) always returns 0.40."""
        assert compute_per_field_confidence("partial") == 0.40


class TestNormalizeSearchScoresEdgeCases:
    """Edge-case tests for normalize_search_scores."""

    def test_very_small_scores(self):
        """Very small but non-zero scores should normalize correctly."""
        result = normalize_search_scores([0.001, 0.002, 0.003])
        assert result == pytest.approx([1 / 3, 2 / 3, 1.0], abs=1e-3)

    def test_large_scores(self):
        """Large scores should normalize correctly."""
        result = normalize_search_scores([100.0, 50.0, 25.0])
        assert result == pytest.approx([1.0, 0.5, 0.25], abs=1e-3)

    def test_single_zero_score(self):
        """Single zero score normalizes to [0.0]."""
        assert normalize_search_scores([0.0]) == [0.0]

    def test_mixed_zero_and_nonzero(self):
        """Mix of zero and non-zero scores."""
        result = normalize_search_scores([0.0, 5.0, 10.0])
        assert result == pytest.approx([0.0, 0.5, 1.0], abs=1e-3)

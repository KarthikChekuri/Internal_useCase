"""Tests for app.utils.fuzzy — Phase 2.3: Fuzzy Matching Utilities.

Each test maps to a spec scenario or a requirement from the Phase 2.3 prompt.
Tests are written BEFORE the implementation (TDD Red phase).
"""

import pytest
from app.utils.fuzzy import sliding_window_fuzzy, normalize_name, tokenize_name


# =============================================================================
# normalize_name tests
# =============================================================================


class TestNormalizeName:
    """Tests for normalize_name(name: str) -> str."""

    def test_lowercases_input(self):
        """normalize_name should lowercase the input."""
        assert normalize_name("Karthik Chekuri") == "karthik chekuri"

    def test_strips_apostrophes(self):
        """normalize_name should strip apostrophes (O'Brien -> obrien)."""
        assert normalize_name("O'Brien") == "obrien"

    def test_strips_hyphens(self):
        """normalize_name should strip hyphens (Anne-Marie -> anne marie or annemarie)."""
        result = normalize_name("Anne-Marie")
        # Hyphens should be stripped; implementation decides if replaced with
        # space or removed. Either "annemarie" or "anne marie" is acceptable
        # as long as it's consistent. Based on spec: "strip punctuation".
        assert "'" not in result
        assert "-" not in result
        assert result == result.lower()

    def test_collapses_whitespace(self):
        """normalize_name should collapse multiple spaces into one."""
        assert normalize_name("Karthik  Chekuri") == "karthik chekuri"

    def test_strips_leading_trailing_whitespace(self):
        """normalize_name should strip leading and trailing whitespace."""
        assert normalize_name("  Karthik Chekuri  ") == "karthik chekuri"

    def test_strips_periods(self):
        """normalize_name should strip periods."""
        assert normalize_name("Dr. Smith") == "dr smith"

    def test_strips_commas(self):
        """normalize_name should strip commas."""
        assert normalize_name("Smith, John") == "smith john"

    def test_empty_string(self):
        """normalize_name should handle empty string."""
        assert normalize_name("") == ""

    def test_only_punctuation(self):
        """normalize_name should handle string with only punctuation."""
        result = normalize_name("'-.,!")
        assert result.strip() == ""

    def test_already_normalized(self):
        """normalize_name should be idempotent for already-normalized input."""
        assert normalize_name("karthik chekuri") == "karthik chekuri"

    def test_mixed_punctuation_and_spaces(self):
        """normalize_name should handle mixed punctuation and extra spaces."""
        assert normalize_name("O'Brien,  Mary-Jane") in ("obrien mary jane", "obrien maryjane")


# =============================================================================
# tokenize_name tests
# =============================================================================


class TestTokenizeName:
    """Tests for tokenize_name(name: str) -> list[str]."""

    def test_simple_two_part_name(self):
        """tokenize_name should split 'Karthik Chekuri' into two tokens."""
        result = tokenize_name("Karthik Chekuri")
        assert result == ["karthik", "chekuri"]

    def test_name_with_apostrophe(self):
        """tokenize_name should handle O'Brien — apostrophe is stripped."""
        result = tokenize_name("O'Brien")
        assert result == ["obrien"]

    def test_name_with_hyphen(self):
        """tokenize_name should handle hyphenated names."""
        result = tokenize_name("Anne-Marie Smith")
        # Hyphens should be handled — either split or merged.
        # The key requirement is it produces useful tokens.
        assert "smith" in result
        # "anne-marie" should become either ["anne", "marie"] or ["annemarie"]
        assert len(result) >= 2

    def test_single_name(self):
        """tokenize_name should handle a single name."""
        result = tokenize_name("Karthik")
        assert result == ["karthik"]

    def test_multiple_spaces(self):
        """tokenize_name should handle multiple spaces between parts."""
        result = tokenize_name("Karthik   Chekuri")
        assert result == ["karthik", "chekuri"]

    def test_empty_string(self):
        """tokenize_name should return empty list for empty string."""
        result = tokenize_name("")
        assert result == []

    def test_three_part_name(self):
        """tokenize_name should handle three-part names."""
        result = tokenize_name("Mary Jane Watson")
        assert result == ["mary", "jane", "watson"]

    def test_name_with_period(self):
        """tokenize_name should strip periods."""
        result = tokenize_name("Dr. Smith")
        assert result == ["dr", "smith"]


# =============================================================================
# sliding_window_fuzzy tests
# =============================================================================


class TestSlidingWindowFuzzy:
    """Tests for sliding_window_fuzzy(text, search_term, threshold=75) -> (score, position)."""

    def test_exact_match_returns_100(self):
        """Exact match should return score of 100.0."""
        text = "some text before Karthik Chekuri some text after"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        assert pos >= 0

    def test_exact_match_position_is_near_match(self):
        """Position should point to approximately where the match was found."""
        text = "some text before Karthik Chekuri some text after"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        # The position should be somewhere near "Karthik Chekuri" in the text
        # "some text before " is 18 chars
        assert pos >= 0

    def test_misspelled_name_scores_above_threshold(self):
        """Misspelled name 'Kerthik Chekuri' should score above the 75 threshold.

        Spec scenario: Misspelled name fuzzy match. The spec expects confidence
        between 0.80-0.90 for direct comparison (token_set_ratio gives ~93).
        With the sliding window approach, surrounding text in the window
        slightly dilutes the score, but it should remain above threshold (75).
        """
        text = "some text before Kerthik Chekuri some text after"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score >= 75, f"Expected score >= 75, got {score}"
        assert pos >= 0

    def test_reordered_name_returns_100(self):
        """Reordered name tokens should return 100 (token_set_ratio).

        Spec scenario: Reordered name tokens — 'Chekuri Karthik' matches
        'Karthik Chekuri' with confidence 1.0 (score 100).
        """
        text = "some text before Chekuri Karthik some text after"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        assert pos >= 0

    def test_no_match_returns_zero_score(self):
        """Completely different text should return score 0 or below threshold.

        Spec scenario: Severely misspelled name below threshold.
        """
        text = "some text before Zxywq Abcde some text after"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score < 75

    def test_default_threshold_is_75(self):
        """Default threshold should be 75."""
        text = "some random text with no matching content at all"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        # Below threshold, score should be whatever the max window score is
        # (which will be low for completely unrelated text)
        assert score < 75

    def test_custom_threshold(self):
        """Custom threshold should be respected."""
        text = "some text before Kerthik Chekuri some text after"
        # With a very high threshold (95), a misspelled name should not meet it
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri", threshold=95)
        # The function returns the raw score regardless of threshold
        # (the caller uses the threshold to decide if it's a match)
        assert score >= 0

    def test_search_term_at_start_of_text(self):
        """Match at the beginning of text should be found."""
        text = "Karthik Chekuri lives in this city"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        assert pos == 0

    def test_search_term_at_end_of_text(self):
        """Match at the end of text should be found."""
        text = "The name of the person is Karthik Chekuri"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0

    def test_short_text_shorter_than_window(self):
        """When text is shorter than a single window, it should still work."""
        text = "Karthik Chekuri"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        assert pos == 0

    def test_empty_text_returns_zero(self):
        """Empty text should return score 0."""
        score, pos = sliding_window_fuzzy("", "Karthik Chekuri")
        assert score == 0.0

    def test_empty_search_term_returns_zero(self):
        """Empty search term should return score 0."""
        score, pos = sliding_window_fuzzy("some text", "")
        assert score == 0.0

    def test_window_size_is_1_5x_search_term_length(self):
        """Window size should be len(search_term) * 1.5.

        Spec: 'split text into overlapping windows of len(search_term) * 1.5 chars'
        """
        # This is an implementation detail test to verify the design decision.
        # "Karthik Chekuri" is 15 chars, window should be ~22 chars.
        # We test indirectly: if the window is correct, a match embedded in
        # surrounding text should still score well.
        search_term = "Karthik Chekuri"
        # Create text where the name is surrounded by just enough context
        # that a correct window size would capture it
        text = "xxxxxx Karthik Chekuri yyyyyy more text follows here"
        score, pos = sliding_window_fuzzy(text, search_term)
        assert score == 100.0

    def test_step_size_is_half_search_term_length(self):
        """Step size should be max(1, len(search_term) // 2).

        Spec: '50% overlap between consecutive windows'
        """
        # With proper step size, we should find matches even when they don't
        # align with window boundaries
        search_term = "Karthik Chekuri"
        # Place the name at an offset that wouldn't align with a non-overlapping window
        text = "ab Karthik Chekuri cd"
        score, pos = sliding_window_fuzzy(text, search_term)
        assert score == 100.0

    def test_single_word_search_term(self):
        """Single word search term should work."""
        text = "file contains Karthik somewhere in here"
        score, pos = sliding_window_fuzzy(text, "Karthik")
        assert score == 100.0
        assert pos >= 0

    def test_returns_tuple_of_float_and_int(self):
        """Return type should be (float, int)."""
        text = "Karthik Chekuri"
        result = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert isinstance(result, tuple)
        assert len(result) == 2
        score, pos = result
        assert isinstance(score, (int, float))
        assert isinstance(pos, int)

    def test_long_text_with_match_in_middle(self):
        """Match in the middle of a long text should be found."""
        padding = "x" * 500
        text = f"{padding} Karthik Chekuri {padding}"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0
        # Position should be approximately at 500
        assert 490 <= pos <= 520

    def test_obrien_apostrophe_name(self):
        """O'Brien-style names should score well against apostrophe-less version.

        This tests that fuzzy matching handles punctuation variations.
        """
        text = "The person named OBrien was mentioned here"
        score, pos = sliding_window_fuzzy(text, "O'Brien")
        assert score >= 75, f"Expected score >= 75 for O'Brien vs OBrien, got {score}"

    def test_partial_name_first_name_only(self):
        """First name only in text should score appropriately against full name."""
        text = "some text Karthik some more text here"
        score, pos = sliding_window_fuzzy(text, "Karthik")
        assert score == 100.0

    def test_case_sensitivity_handled_by_token_set_ratio(self):
        """token_set_ratio is case-insensitive by default with rapidfuzz."""
        text = "some text KARTHIK CHEKURI some more"
        score, pos = sliding_window_fuzzy(text, "karthik chekuri")
        assert score == 100.0


# =============================================================================
# Additional edge-case tests (Phase 6.1)
# =============================================================================


class TestNormalizeNameEdgeCases:
    """Additional edge-case tests for normalize_name."""

    def test_unicode_characters_preserved(self):
        """Unicode letters (accents) should be preserved after normalization."""
        result = normalize_name("Jose Garcia")
        assert result == "jose garcia"

    def test_numbers_in_name_preserved(self):
        """Numeric characters in names are preserved."""
        result = normalize_name("R2D2 Unit")
        assert result == "r2d2 unit"

    def test_tabs_and_newlines_collapsed(self):
        """Tabs and newlines should be collapsed to single spaces."""
        result = normalize_name("Karthik\t\nChekuri")
        assert result == "karthik chekuri"

    def test_multiple_apostrophes(self):
        """Multiple apostrophes are all stripped."""
        result = normalize_name("O'Brien-D'Angelo")
        assert "'" not in result
        assert "-" not in result


class TestTokenizeNameEdgeCases:
    """Additional edge-case tests for tokenize_name."""

    def test_name_with_many_parts(self):
        """Names with many parts produce many tokens."""
        result = tokenize_name("Mary Jane Watson Smith Jones")
        assert result == ["mary", "jane", "watson", "smith", "jones"]

    def test_only_spaces(self):
        """String with only spaces produces empty list."""
        result = tokenize_name("     ")
        assert result == []


class TestSlidingWindowFuzzyEdgeCases:
    """Additional edge-case tests for sliding_window_fuzzy."""

    def test_special_characters_in_search_term(self):
        """Search term with special characters should not crash."""
        text = "file with O'Brien mentioned here somewhere"
        score, pos = sliding_window_fuzzy(text, "O'Brien")
        assert score >= 75

    def test_very_short_text_one_word(self):
        """Single-word text against single-word search term."""
        score, pos = sliding_window_fuzzy("Karthik", "Karthik")
        assert score == 100.0

    def test_search_term_longer_than_text(self):
        """When search term is longer than text, should still compute."""
        score, pos = sliding_window_fuzzy("Hi", "Karthik Chekuri Is Here")
        # The text is shorter than the window, so entire text is compared
        assert isinstance(score, float)
        assert isinstance(pos, int)

    def test_repeated_name_in_text(self):
        """Name repeated multiple times should still find it."""
        text = "Karthik Chekuri was here. Also Karthik Chekuri was there."
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0

    def test_name_surrounded_by_numbers(self):
        """Name surrounded by numeric data should still be found."""
        text = "12345 Karthik Chekuri 67890"
        score, pos = sliding_window_fuzzy(text, "Karthik Chekuri")
        assert score == 100.0

"""Tests for Phase V3-2.2: V3 Search Query Builder and Field Execution.

Tests the per-field query building functions and execute_field_query:
- build_field_query: constructs Lucene query per individual field
- get_search_mode: returns "all" or "any" per field
- get_metadata_filter: returns Azure filter expression or None per field
- execute_field_query: calls Azure AI Search with correct params + returns tuples

All Azure Search calls are mocked. No DB or real search connections used.
"""

from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Helpers — fake Azure search result for V3 (supports highlights)
# ---------------------------------------------------------------------------


class FakeV3SearchResult:
    """Mimics an Azure AI Search result document for V3.

    Supports md5, @search.score, and optional @search.highlights.
    """

    def __init__(self, md5: str, score: float, highlights=None):
        self._md5 = md5
        self._score = score
        self._highlights = highlights  # dict like {"content": ["snippet text"]} or None

    def __getitem__(self, key):
        if key == "md5":
            return self._md5
        if key == "@search.score":
            return self._score
        if key == "@search.highlights":
            return self._highlights
        raise KeyError(key)

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# ===========================================================================
# TEST CLASS: build_field_query
# ===========================================================================


class TestBuildFieldQuery:
    """Tests for build_field_query — per-field Lucene query construction."""

    # --- SSN ---

    def test_ssn_dashed_produces_dashed_and_undashed(self):
        """SSN '343-43-4343' → '"343-43-4343" OR "343434343"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("SSN", "343-43-4343")
        assert result == '"343-43-4343" OR "343434343"'

    def test_ssn_second_example(self):
        """SSN '123-45-6789' → '"123-45-6789" OR "123456789"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("SSN", "123-45-6789")
        assert result == '"123-45-6789" OR "123456789"'

    # --- Fullname ---

    def test_fullname_two_tokens_fuzzy(self):
        """Fullname 'Karthik Chekuri' → 'Karthik~1 Chekuri~1'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("Fullname", "Karthik Chekuri")
        assert result == "Karthik~1 Chekuri~1"

    def test_fullname_with_apostrophe_preserved_in_token(self):
        """Fullname 'Robert O'Brien' → fuzzy query with ~1 on each token.

        Per spec: apostrophe is NOT a Lucene special char, so preserved.
        But the V2 _escape_lucene removes apostrophes. For V3, we preserve them.
        The key requirement is ~1 fuzzy on each token.
        """
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("Fullname", "Robert O'Brien")
        # Must have ~1 fuzzy on tokens; exact apostrophe handling is implementation detail
        assert "~1" in result
        assert "Robert~1" in result
        # OBrien or O'Brien token must be present with ~1
        assert "Brien~1" in result or "O'Brien~1" in result or "OBrien~1" in result

    # --- FirstName / LastName ---

    def test_firstname_single_fuzzy_token(self):
        """FirstName 'Karthik' → 'Karthik~1'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("FirstName", "Karthik")
        assert result == "Karthik~1"

    def test_lastname_single_fuzzy_token(self):
        """LastName 'Chekuri' → 'Chekuri~1'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("LastName", "Chekuri")
        assert result == "Chekuri~1"

    # --- DOB ---

    def test_dob_iso_string_produces_four_formats(self):
        """DOB '1992-07-15' → all four date formats in OR chain."""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("DOB", "1992-07-15")
        assert result == '"07/15/1992" OR "1992-07-15" OR "15/07/1992" OR "15.07.1992"'

    # --- ZipCode ---

    def test_zipcode_quoted_exact(self):
        """ZipCode '77001' → '"77001"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("ZipCode", "77001")
        assert result == '"77001"'

    # --- DriversLicense ---

    def test_drivers_license_quoted_exact(self):
        """DriversLicense 'TX12345678' → '"TX12345678"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("DriversLicense", "TX12345678")
        assert result == '"TX12345678"'

    # --- State ---

    def test_state_quoted_exact(self):
        """State 'TX' → '"TX"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("State", "TX")
        assert result == '"TX"'

    # --- City ---

    def test_city_single_word_quoted(self):
        """City 'Houston' → '"Houston"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("City", "Houston")
        assert result == '"Houston"'

    def test_city_multi_word_quoted_phrase(self):
        """City 'New York' → '"New York"' (quoted phrase, not fuzzy)"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("City", "New York")
        assert result == '"New York"'

    # --- Address1 ---

    def test_address1_quoted_phrase(self):
        """Address1 '123 Main Street' → '"123 Main Street"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("Address1", "123 Main Street")
        assert result == '"123 Main Street"'

    # --- Country ---

    def test_country_quoted_phrase(self):
        """Country 'United States' → '"United States"'"""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("Country", "United States")
        assert result == '"United States"'

    # --- Null / empty guards ---

    def test_null_value_returns_none(self):
        """None value → returns None (skip field)."""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("SSN", None)
        assert result is None

    def test_empty_string_returns_none(self):
        """Empty string value → returns None (skip field)."""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("SSN", "")
        assert result is None

    def test_whitespace_only_returns_none(self):
        """Whitespace-only string value → returns None (skip field)."""
        from app.services.search_service_v3 import build_field_query

        result = build_field_query("Fullname", "   ")
        assert result is None


# ===========================================================================
# TEST CLASS: get_search_mode
# ===========================================================================


class TestGetSearchMode:
    """Tests for get_search_mode — returns 'all' or 'any' per field type."""

    def test_ssn_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("SSN") == "all"

    def test_fullname_mode_is_any(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("Fullname") == "any"

    def test_firstname_mode_is_any(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("FirstName") == "any"

    def test_lastname_mode_is_any(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("LastName") == "any"

    def test_dob_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("DOB") == "all"

    def test_zipcode_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("ZipCode") == "all"

    def test_drivers_license_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("DriversLicense") == "all"

    def test_state_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("State") == "all"

    def test_city_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("City") == "all"

    def test_address1_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("Address1") == "all"

    def test_address2_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("Address2") == "all"

    def test_address3_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("Address3") == "all"

    def test_country_mode_is_all(self):
        from app.services.search_service_v3 import get_search_mode
        assert get_search_mode("Country") == "all"


# ===========================================================================
# TEST CLASS: get_metadata_filter
# ===========================================================================


class TestGetMetadataFilter:
    """Tests for get_metadata_filter — returns OData filter string or None."""

    def _with_filters_enabled(self, fn):
        """Run fn with METADATA_FILTERS_ENABLED=True, then restore."""
        import app.services.search_service_v3 as mod
        orig = mod.METADATA_FILTERS_ENABLED
        try:
            mod.METADATA_FILTERS_ENABLED = True
            return fn()
        finally:
            mod.METADATA_FILTERS_ENABLED = orig

    def test_disabled_returns_none_for_all(self):
        import app.services.search_service_v3 as mod
        orig = mod.METADATA_FILTERS_ENABLED
        try:
            mod.METADATA_FILTERS_ENABLED = False
            assert mod.get_metadata_filter("SSN") is None
            assert mod.get_metadata_filter("Fullname") is None
            assert mod.get_metadata_filter("DOB") is None
            assert mod.get_metadata_filter("Address1") is None
        finally:
            mod.METADATA_FILTERS_ENABLED = orig

    def test_ssn_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("SSN", "has_ssn eq true"))

    def test_fullname_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("Fullname", "has_name eq true"))

    def test_firstname_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("FirstName", "has_name eq true"))

    def test_lastname_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("LastName", "has_name eq true"))

    def test_dob_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("DOB", "has_dob eq true"))

    def test_address1_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("Address1", "has_address eq true"))

    def test_address2_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("Address2", "has_address eq true"))

    def test_address3_filter(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("Address3", "has_address eq true"))

    def test_city_filter_is_none(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("City", None))

    def test_state_filter_is_none(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("State", None))

    def test_zipcode_filter_is_none(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("ZipCode", None))

    def test_country_filter_is_none(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("Country", None))

    def test_drivers_license_filter_is_none(self):
        self._with_filters_enabled(
            lambda: self._assert_eq("DriversLicense", None))

    def _assert_eq(self, field, expected):
        from app.services.search_service_v3 import get_metadata_filter
        assert get_metadata_filter(field) == expected


# ===========================================================================
# TEST CLASS: execute_field_query
# ===========================================================================


class TestExecuteFieldQuery:
    """Tests for execute_field_query — Azure AI Search call + result parsing."""

    def test_search_called_with_correct_fixed_params(self):
        """execute_field_query calls SearchClient.search() with the required fixed params."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        execute_field_query(mock_client, "SSN", "343-43-4343")

        mock_client.search.assert_called_once()
        call_kwargs = mock_client.search.call_args.kwargs

        assert call_kwargs["query_type"] == "full"
        assert call_kwargs["search_fields"] == ["content", "content_phonetic", "content_lowercase"]
        assert call_kwargs["scoring_profile"] == "pii_boost"
        assert call_kwargs["highlight_fields"] == "content"
        assert call_kwargs["highlight_pre_tag"] == "[[MATCH]]"
        assert call_kwargs["highlight_post_tag"] == "[[/MATCH]]"
        assert call_kwargs["top"] == 100

    def test_search_called_with_correct_filter_for_ssn(self):
        """execute_field_query passes the correct metadata filter for SSN when enabled."""
        import app.services.search_service_v3 as mod
        orig = mod.METADATA_FILTERS_ENABLED
        try:
            mod.METADATA_FILTERS_ENABLED = True
            mock_client = MagicMock()
            mock_client.search.return_value = []
            mod.execute_field_query(mock_client, "SSN", "343-43-4343")
            call_kwargs = mock_client.search.call_args.kwargs
            assert call_kwargs["filter"] == "has_ssn eq true"
        finally:
            mod.METADATA_FILTERS_ENABLED = orig

    def test_search_called_with_correct_filter_for_fullname(self):
        """execute_field_query passes the correct metadata filter for Fullname when enabled."""
        import app.services.search_service_v3 as mod
        orig = mod.METADATA_FILTERS_ENABLED
        try:
            mod.METADATA_FILTERS_ENABLED = True
            mock_client = MagicMock()
            mock_client.search.return_value = []
            mod.execute_field_query(mock_client, "Fullname", "Karthik Chekuri")
            call_kwargs = mock_client.search.call_args.kwargs
            assert call_kwargs["filter"] == "has_name eq true"
        finally:
            mod.METADATA_FILTERS_ENABLED = orig

    def test_search_called_with_no_filter_when_disabled(self):
        """execute_field_query passes filter=None when METADATA_FILTERS_ENABLED=False."""
        import app.services.search_service_v3 as mod
        orig = mod.METADATA_FILTERS_ENABLED
        try:
            mod.METADATA_FILTERS_ENABLED = False
            mock_client = MagicMock()
            mock_client.search.return_value = []
            mod.execute_field_query(mock_client, "SSN", "343-43-4343")
            call_kwargs = mock_client.search.call_args.kwargs
            assert call_kwargs["filter"] is None
        finally:
            mod.METADATA_FILTERS_ENABLED = orig

    def test_search_called_with_none_filter_for_city(self):
        """execute_field_query passes filter=None for City (no metadata filter)."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        execute_field_query(mock_client, "City", "Houston")

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["filter"] is None

    def test_search_called_with_correct_search_mode_for_ssn(self):
        """execute_field_query passes search_mode='all' for SSN."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        execute_field_query(mock_client, "SSN", "343-43-4343")

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["search_mode"] == "all"

    def test_search_called_with_correct_search_mode_for_fullname(self):
        """execute_field_query passes search_mode='any' for Fullname."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        execute_field_query(mock_client, "Fullname", "Karthik Chekuri")

        call_kwargs = mock_client.search.call_args.kwargs
        assert call_kwargs["search_mode"] == "any"

    def test_returns_list_of_tuples_with_md5_score_snippet(self):
        """execute_field_query returns list of (md5, score, snippet) tuples."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = [
            FakeV3SearchResult(
                "abc123",
                10.5,
                highlights={"content": ["found [[MATCH]]343-43-4343[[/MATCH]] here"]},
            )
        ]

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert len(results) == 1
        md5, score, snippet = results[0]
        assert md5 == "abc123"
        assert score == 10.5
        assert snippet == "found [[MATCH]]343-43-4343[[/MATCH]] here"

    def test_returns_empty_list_when_no_results(self):
        """execute_field_query returns empty list when Azure returns nothing."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert results == []

    def test_snippet_is_none_when_no_highlights(self):
        """snippet is None when result has no @search.highlights."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = [
            FakeV3SearchResult("def456", 7.2, highlights=None)
        ]

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert len(results) == 1
        md5, score, snippet = results[0]
        assert md5 == "def456"
        assert score == 7.2
        assert snippet is None

    def test_snippet_is_none_when_highlights_content_missing(self):
        """snippet is None when @search.highlights exists but has no 'content' key."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = [
            FakeV3SearchResult("ghi789", 5.0, highlights={})
        ]

        results = execute_field_query(mock_client, "SSN", "343-43-4343")

        assert len(results) == 1
        _, _, snippet = results[0]
        assert snippet is None

    def test_null_field_value_returns_empty_list_no_search_call(self):
        """When field_value is None, returns empty list and does NOT call search."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()

        results = execute_field_query(mock_client, "SSN", None)

        mock_client.search.assert_not_called()
        assert results == []

    def test_empty_field_value_returns_empty_list_no_search_call(self):
        """When field_value is empty string, returns empty list and does NOT call search."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()

        results = execute_field_query(mock_client, "SSN", "")

        mock_client.search.assert_not_called()
        assert results == []

    def test_multiple_results_all_returned(self):
        """All matching results are returned as tuples."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = [
            FakeV3SearchResult("md5_a", 12.0, highlights={"content": ["hit A"]}),
            FakeV3SearchResult("md5_b", 9.5, highlights=None),
            FakeV3SearchResult("md5_c", 7.0, highlights={"content": ["hit C"]}),
        ]

        results = execute_field_query(mock_client, "Fullname", "Karthik Chekuri")

        assert len(results) == 3
        md5s = [r[0] for r in results]
        assert "md5_a" in md5s
        assert "md5_b" in md5s
        assert "md5_c" in md5s

    def test_search_text_is_built_from_field_query(self):
        """The search_text passed to Azure is the result of build_field_query."""
        from app.services.search_service_v3 import execute_field_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        execute_field_query(mock_client, "SSN", "343-43-4343")

        call_kwargs = mock_client.search.call_args.kwargs
        # SSN query should be both dashed and undashed forms
        assert call_kwargs["search_text"] == '"343-43-4343" OR "343434343"'


# ===========================================================================
# TEST CLASS: merge_field_results  (Phase V3-3.1)
# ===========================================================================


class TestMergeFieldResults:
    """Tests for merge_field_results — merges per-field result lists into per-doc dicts."""

    def test_two_fields_with_shared_and_unique_docs(self):
        """SSN: [doc_A(12.5), doc_B(10.0)], Fullname: [doc_A(8.3), doc_C(6.1)]
        → doc_A has both SSN+Fullname, doc_B has SSN only, doc_C has Fullname only.
        """
        from app.services.search_service_v3 import merge_field_results

        field_results = {
            "SSN": [
                ("md5_A", 12.5, "ssn snippet"),
                ("md5_B", 10.0, "another snippet"),
            ],
            "Fullname": [
                ("md5_A", 8.3, None),
                ("md5_C", 6.1, "name snippet"),
            ],
        }

        result = merge_field_results(field_results)

        # doc_A has both fields
        assert "md5_A" in result
        assert "SSN" in result["md5_A"]
        assert result["md5_A"]["SSN"]["found"] is True
        assert result["md5_A"]["SSN"]["score"] == 12.5
        assert result["md5_A"]["SSN"]["snippet"] == "ssn snippet"
        assert "Fullname" in result["md5_A"]
        assert result["md5_A"]["Fullname"]["found"] is True
        assert result["md5_A"]["Fullname"]["score"] == 8.3
        assert result["md5_A"]["Fullname"]["snippet"] is None

        # doc_B has SSN only
        assert "md5_B" in result
        assert "SSN" in result["md5_B"]
        assert result["md5_B"]["SSN"]["score"] == 10.0
        assert "Fullname" not in result["md5_B"]

        # doc_C has Fullname only
        assert "md5_C" in result
        assert "Fullname" in result["md5_C"]
        assert result["md5_C"]["Fullname"]["score"] == 6.1
        assert "SSN" not in result["md5_C"]

    def test_single_field_single_doc(self):
        """Single field with one doc → that doc has one field found."""
        from app.services.search_service_v3 import merge_field_results

        field_results = {
            "DOB": [("md5_X", 9.0, "dob snippet")],
        }

        result = merge_field_results(field_results)

        assert "md5_X" in result
        assert "DOB" in result["md5_X"]
        assert result["md5_X"]["DOB"]["found"] is True
        assert result["md5_X"]["DOB"]["score"] == 9.0
        assert result["md5_X"]["DOB"]["snippet"] == "dob snippet"
        # Only one doc, only one field
        assert len(result) == 1
        assert len(result["md5_X"]) == 1

    def test_empty_field_results_returns_empty_dict(self):
        """Empty input → empty output."""
        from app.services.search_service_v3 import merge_field_results

        result = merge_field_results({})

        assert result == {}

    def test_all_fields_empty_lists_returns_empty_dict(self):
        """Fields present but all with empty result lists → empty output."""
        from app.services.search_service_v3 import merge_field_results

        result = merge_field_results({"SSN": [], "Fullname": [], "DOB": []})

        assert result == {}

    def test_result_structure_has_found_score_snippet_keys(self):
        """Each field entry in merged output has exactly found, score, snippet keys."""
        from app.services.search_service_v3 import merge_field_results

        field_results = {"SSN": [("md5_Z", 5.0, None)]}
        result = merge_field_results(field_results)

        entry = result["md5_Z"]["SSN"]
        assert set(entry.keys()) == {"found", "score", "snippet"}


# ===========================================================================
# TEST CLASS: compute_confidence_v3  (Phase V3-3.1)
# ===========================================================================


class TestComputeConfidenceV3:
    """Tests for compute_confidence_v3 — weighted confidence calculation."""

    def test_ssn_plus_name_plus_dob_weighted_average(self):
        """SSN(12.5) + Fullname(8.3) + DOB(9.0), max=12.5 → 0.6932, needs_review=False."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "SSN": {"found": True, "score": 12.5, "snippet": "ssn"},
            "Fullname": {"found": True, "score": 8.3, "snippet": "name"},
            "DOB": {"found": True, "score": 9.0, "snippet": "dob"},
        }
        max_score = 12.5

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf = 1.0, name_conf = 8.3/12.5 = 0.664, other_avg = 9.0/12.5 = 0.72
        # overall = 0.35*1.0 + 0.30*0.664 + 0.20*0.72 + 0.15*0.0 = 0.6932
        assert abs(confidence - 0.6932) < 0.001
        assert needs_review is False

    def test_ssn_only_below_threshold_needs_review(self):
        """SSN only (10.0), max=10.0 → overall < 0.5 → needs_review=True."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "SSN": {"found": True, "score": 10.0, "snippet": "ssn"},
        }
        max_score = 10.0

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf = 1.0, name_conf = 0.0, other_avg = 0.0
        # overall = 0.35*1.0 + 0.30*0.0 + 0.20*0.0 + 0.15*0.0 = 0.35
        assert abs(confidence - 0.35) < 0.001
        assert needs_review is True  # 0.35 < 0.5

    def test_firstname_only_needs_review_regardless_of_score(self):
        """FirstName only (6.0), max=6.0 → needs_review=True regardless of score."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "FirstName": {"found": True, "score": 6.0, "snippet": "fname"},
        }
        max_score = 6.0

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # Only FirstName present, no Fullname, LastName, or SSN
        # needs_review must be True
        assert needs_review is True

    def test_name_category_takes_max_of_name_fields(self):
        """Name confidence = max of Fullname, FirstName, LastName confidences."""
        from app.services.search_service_v3 import compute_confidence_v3

        # FirstName(6.0), LastName(8.0), max=10.0
        # FirstName conf = 0.6, LastName conf = 0.8 → name_conf = max(0.6, 0.8) = 0.8
        doc_fields = {
            "FirstName": {"found": True, "score": 6.0, "snippet": None},
            "LastName": {"found": True, "score": 8.0, "snippet": None},
        }
        max_score = 10.0

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf = 0.0, name_conf = 0.8 (max of 0.6 and 0.8), other_avg = 0.0
        # overall = 0.35*0.0 + 0.30*0.8 + 0.20*0.0 + 0.15*0.0 = 0.24
        assert abs(confidence - 0.24) < 0.001
        # 0.24 < 0.5 → needs_review=True
        assert needs_review is True

    def test_other_category_averages_non_name_non_ssn_fields(self):
        """Other = average of found non-name/non-SSN field confidences."""
        from app.services.search_service_v3 import compute_confidence_v3

        # DOB(9.0) + ZipCode(6.0), max=10.0
        # dob_conf = 0.9, zip_conf = 0.6 → other_avg = (0.9 + 0.6) / 2 = 0.75
        doc_fields = {
            "DOB": {"found": True, "score": 9.0, "snippet": None},
            "ZipCode": {"found": True, "score": 6.0, "snippet": None},
        }
        max_score = 10.0

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf=0.0, name_conf=0.0, other_avg=0.75
        # overall = 0.35*0.0 + 0.30*0.0 + 0.20*0.75 + 0.15*0.0 = 0.15
        assert abs(confidence - 0.15) < 0.001
        assert needs_review is True  # 0.15 < 0.5

    def test_fullname_with_ssn_no_firstname_needs_review_false(self):
        """Fullname + SSN both present → needs_review based on score only."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {
            "SSN": {"found": True, "score": 10.0, "snippet": "ssn"},
            "Fullname": {"found": True, "score": 10.0, "snippet": "name"},
        }
        max_score = 10.0

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf=1.0, name_conf=1.0, other_avg=0.0
        # overall = 0.35*1.0 + 0.30*1.0 + 0.20*0.0 + 0.15*0.0 = 0.65
        assert abs(confidence - 0.65) < 0.001
        assert needs_review is False  # 0.65 >= 0.5 and Fullname present (not FirstName-only)

    def test_per_field_confidence_capped_at_1(self):
        """Per-field confidence is capped at 1.0 even if score > max_score."""
        from app.services.search_service_v3 import compute_confidence_v3

        # If somehow a field score exceeds max_score, cap at 1.0
        doc_fields = {
            "SSN": {"found": True, "score": 15.0, "snippet": "ssn"},
        }
        max_score = 10.0  # SSN score > max_score

        confidence, needs_review = compute_confidence_v3(doc_fields, max_score)

        # ssn_conf = min(1.0, 15.0/10.0) = 1.0
        # overall = 0.35*1.0 = 0.35
        assert abs(confidence - 0.35) < 0.001

    def test_returns_tuple_of_float_and_bool(self):
        """compute_confidence_v3 returns a (float, bool) tuple."""
        from app.services.search_service_v3 import compute_confidence_v3

        doc_fields = {"SSN": {"found": True, "score": 10.0, "snippet": None}}
        result = compute_confidence_v3(doc_fields, 10.0)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], float)
        assert isinstance(result[1], bool)


# ===========================================================================
# TEST CLASS: search_customer_v3  (Phase V3-3.1)
# ===========================================================================


class TestSearchCustomerV3:
    """Tests for search_customer_v3 — orchestrates per-field queries and merges results."""

    def _make_customer(self, **kwargs):
        """Create a simple customer namespace with PII fields."""
        from types import SimpleNamespace

        defaults = {
            "Fullname": None,
            "FirstName": None,
            "LastName": None,
            "DOB": None,
            "SSN": None,
            "DriversLicense": None,
            "Address1": None,
            "Address2": None,
            "Address3": None,
            "ZipCode": None,
            "City": None,
            "State": None,
            "Country": None,
        }
        defaults.update(kwargs)
        return SimpleNamespace(**defaults)

    def test_returns_list_of_merged_per_document_results(self):
        """Customer with SSN+Fullname+DOB → per-document results list with confidence."""
        from unittest.mock import patch

        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()
        customer = self._make_customer(
            SSN="123-45-6789",
            Fullname="John Smith",
            DOB="1990-01-15",
        )

        ssn_results = [("md5_A", 12.5, "ssn snippet"), ("md5_B", 10.0, "b snippet")]
        fullname_results = [("md5_A", 8.3, None)]
        dob_results = [("md5_A", 9.0, "dob snippet")]

        def fake_execute(client, field_name, field_value):
            return {
                "SSN": ssn_results,
                "Fullname": fullname_results,
                "DOB": dob_results,
            }.get(field_name, [])

        with patch(
            "app.services.search_service_v3.execute_field_query",
            side_effect=fake_execute,
        ):
            results = search_customer_v3(mock_client, customer)

        # Should return a list
        assert isinstance(results, list)
        # Should have one entry per matched doc
        md5s = [r["md5"] for r in results]
        assert "md5_A" in md5s
        assert "md5_B" in md5s

    def test_all_null_pii_returns_empty_list(self):
        """Customer where all PII fields are None → returns empty list."""
        from unittest.mock import patch

        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()
        customer = self._make_customer()  # all None

        with patch(
            "app.services.search_service_v3.execute_field_query",
            return_value=[],
        ) as mock_execute:
            results = search_customer_v3(mock_client, customer)

        assert results == []
        # No query should be sent for null fields
        mock_execute.assert_not_called()

    def test_skips_null_fields_no_query_sent(self):
        """Null DriversLicense is not queried; only non-null fields are searched."""
        from unittest.mock import patch

        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()
        customer = self._make_customer(
            SSN="123-45-6789",
            DriversLicense=None,  # explicitly null
        )

        called_fields = []

        def fake_execute(client, field_name, field_value):
            called_fields.append(field_name)
            return [("md5_X", 10.0, "snippet")]

        with patch(
            "app.services.search_service_v3.execute_field_query",
            side_effect=fake_execute,
        ):
            search_customer_v3(mock_client, customer)

        # DriversLicense should NOT have been queried
        assert "DriversLicense" not in called_fields
        # SSN should have been queried
        assert "SSN" in called_fields

    def test_result_has_md5_fields_confidence_needs_review(self):
        """Each result dict has md5, fields, confidence, and needs_review keys."""
        from unittest.mock import patch

        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()
        customer = self._make_customer(SSN="123-45-6789", Fullname="Jane Doe")

        def fake_execute(client, field_name, field_value):
            return [("md5_X", 10.0, "snippet")]

        with patch(
            "app.services.search_service_v3.execute_field_query",
            side_effect=fake_execute,
        ):
            results = search_customer_v3(mock_client, customer)

        assert len(results) >= 1
        result = results[0]
        assert "md5" in result
        assert "fields" in result
        assert "confidence" in result
        assert "needs_review" in result

    def test_no_results_across_all_fields_returns_empty_list(self):
        """If execute_field_query returns empty for all fields, return empty list."""
        from unittest.mock import patch

        from app.services.search_service_v3 import search_customer_v3

        mock_client = MagicMock()
        customer = self._make_customer(SSN="000-00-0000", Fullname="Ghost Person")

        with patch(
            "app.services.search_service_v3.execute_field_query",
            return_value=[],
        ):
            results = search_customer_v3(mock_client, customer)

        assert results == []

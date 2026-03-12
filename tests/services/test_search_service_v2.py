"""Tests for Phase V2-2.2: Search Service (Strategy-Driven).

Tests the new strategy-driven search service:
- build_query_for_strategy: Lucene query construction per strategy
- load_strategies: YAML loading + validation
- execute_strategy_query: Azure AI Search call with correct params
- search_customer: Multi-strategy union, dedup by MD5, highest score, strategy tracking

All Azure Search calls are mocked. No DB or real search connections used.
"""

import datetime
import os
import tempfile
from dataclasses import dataclass
from typing import Optional
from unittest.mock import MagicMock, patch, call

import pytest
import yaml


# ---------------------------------------------------------------------------
# Helpers — fake customer PII record (avoids sqlalchemy import)
# ---------------------------------------------------------------------------

@dataclass
class FakeCustomer:
    """Lightweight stand-in for the MasterData ORM model."""
    customer_id: int = 1
    Fullname: Optional[str] = "Karthik Chekuri"
    FirstName: Optional[str] = "Karthik"
    LastName: Optional[str] = "Chekuri"
    DOB: Optional[datetime.date] = datetime.date(1990, 5, 15)
    SSN: Optional[str] = "343-43-4343"
    DriversLicense: Optional[str] = "D1234567"
    Address1: Optional[str] = "123 Main St"
    Address2: Optional[str] = None
    Address3: Optional[str] = None
    ZipCode: Optional[str] = "12345"
    City: Optional[str] = "Springfield"
    State: Optional[str] = "IL"
    Country: Optional[str] = "United States"


class FakeSearchResult:
    """Mimics an Azure AI Search result document for V2 (MD5-keyed)."""

    def __init__(self, md5: str, score: float, file_path: str = "case1/file.txt"):
        self._md5 = md5
        self._score = score
        self._file_path = file_path

    def __getitem__(self, key):
        mapping = {
            "md5": self._md5,
            "file_path": self._file_path,
            "@search.score": self._score,
        }
        return mapping[key]

    def get(self, key, default=None):
        try:
            return self[key]
        except KeyError:
            return default


# ===========================================================================
# TEST CLASS: Strategy Query Construction
# ===========================================================================

class TestBuildQueryForStrategy:
    """Tests for build_query_for_strategy (spec: Lucene query construction from strategy fields)."""

    def test_fullname_ssn_strategy_query(self):
        """Scenario: Build query for fullname_ssn strategy.

        GIVEN strategy fullname_ssn with fields [Fullname, SSN]
        WHEN customer has Fullname "Karthik Chekuri" and SSN "343-43-4343"
        THEN Lucene query is: Karthik~1 Chekuri~1 OR "343-43-4343" OR "343434343"
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"])
        customer = FakeCustomer()

        query = build_query_for_strategy(strategy, customer)

        assert "Karthik~1" in query
        assert "Chekuri~1" in query
        assert '"343-43-4343"' in query
        assert '"343434343"' in query

    def test_lastname_dob_strategy_query(self):
        """Scenario: Build query for lastname_dob strategy.

        GIVEN strategy lastname_dob with fields [LastName, DOB]
        WHEN customer has LastName "Chekuri" and DOB 1990-05-15
        THEN query includes: Chekuri~1 OR "1990-05-15" OR "05/15/1990" OR "15/05/1990"
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="lastname_dob", description="", fields=["LastName", "DOB"])
        customer = FakeCustomer()

        query = build_query_for_strategy(strategy, customer)

        assert "Chekuri~1" in query
        assert '"1990-05-15"' in query
        assert '"05/15/1990"' in query
        assert '"15/05/1990"' in query

    def test_unique_identifiers_strategy_query(self):
        """Scenario: Build query for unique_identifiers strategy.

        GIVEN strategy unique_identifiers with fields [SSN, DriversLicense]
        WHEN customer has SSN "343-43-4343" and DriversLicense "D1234567"
        THEN query is: "343-43-4343" OR "343434343" OR "D1234567"
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"])
        customer = FakeCustomer()

        query = build_query_for_strategy(strategy, customer)

        assert '"343-43-4343"' in query
        assert '"343434343"' in query
        assert '"D1234567"' in query

    def test_null_field_omitted_from_query(self):
        """Scenario: Strategy field references a null PII value.

        GIVEN strategy with fields [SSN, DriversLicense] and DriversLicense is NULL
        WHEN the system builds the search query
        THEN DriversLicense term is omitted (only SSN is searched)
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"])
        customer = FakeCustomer(DriversLicense=None)

        query = build_query_for_strategy(strategy, customer)

        assert '"343-43-4343"' in query
        assert '"343434343"' in query
        # DriversLicense should not appear in query
        assert "D1234567" not in query

    def test_all_fields_null_returns_none(self):
        """Scenario: All strategy fields are null for a customer.

        GIVEN strategy with fields [DriversLicense] and DriversLicense is NULL
        WHEN the system builds the search query
        THEN the function returns None (strategy is skipped)
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="dl_only", description="", fields=["DriversLicense"])
        customer = FakeCustomer(DriversLicense=None)

        result = build_query_for_strategy(strategy, customer)

        assert result is None

    def test_hyphenated_name_handled(self):
        """Scenario: Build query for customer with hyphenated name.

        GIVEN strategy fullname_ssn with fields [Fullname, SSN]
        WHEN customer has Fullname "Mary O'Brien-Smith" and SSN "123-45-6789"
        THEN the query handles apostrophe and hyphen, fuzzy operators applied
        """
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"])
        customer = FakeCustomer(
            Fullname="Mary O'Brien-Smith",
            SSN="123-45-6789"
        )

        query = build_query_for_strategy(strategy, customer)

        # Should have fuzzy operators
        assert "~1" in query
        # SSN should be present in both formats
        assert '"123-45-6789"' in query
        assert '"123456789"' in query
        # Name tokens should appear with fuzzy operator (apostrophe/hyphen handled)
        assert "Mary~1" in query

    def test_dob_formats_all_three_included(self):
        """DOB field must produce ISO, US, and European format variants."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["DOB"])
        customer = FakeCustomer(DOB=datetime.date(1990, 5, 15))

        query = build_query_for_strategy(strategy, customer)

        assert '"1990-05-15"' in query  # ISO
        assert '"05/15/1990"' in query  # US
        assert '"15/05/1990"' in query  # European

    def test_drivers_license_quoted_exact(self):
        """DriversLicense should appear as a quoted exact string."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["DriversLicense"])
        customer = FakeCustomer(DriversLicense="D1234567")

        query = build_query_for_strategy(strategy, customer)

        assert '"D1234567"' in query

    def test_zipcode_exact_string(self):
        """ZipCode should appear as an exact string."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["ZipCode"])
        customer = FakeCustomer(ZipCode="12345")

        query = build_query_for_strategy(strategy, customer)

        assert "12345" in query

    def test_state_exact_string(self):
        """State should appear as an exact string."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["State"])
        customer = FakeCustomer(State="IL")

        query = build_query_for_strategy(strategy, customer)

        assert "IL" in query

    def test_city_fuzzy_tokens(self):
        """City should be split into tokens with ~1 fuzzy operator."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["City"])
        customer = FakeCustomer(City="New York")

        query = build_query_for_strategy(strategy, customer)

        assert "New~1" in query
        assert "York~1" in query

    def test_address1_quoted_exact(self):
        """Address1 should appear as a quoted exact string."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["Address1"])
        customer = FakeCustomer(Address1="123 Main St")

        query = build_query_for_strategy(strategy, customer)

        assert '"123 Main St"' in query

    def test_country_quoted_exact(self):
        """Country should appear as a quoted exact string."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["Country"])
        customer = FakeCustomer(Country="United States")

        query = build_query_for_strategy(strategy, customer)

        assert '"United States"' in query

    def test_fields_combined_with_or(self):
        """All field values within a single strategy are combined with OR."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"])
        customer = FakeCustomer()

        query = build_query_for_strategy(strategy, customer)

        # OR must separate parts — the name tokens and SSN parts are OR'd together
        assert "OR" in query

    def test_firstname_fuzzy_tokens(self):
        """FirstName should be split into tokens with ~1 fuzzy operator."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["FirstName"])
        customer = FakeCustomer(FirstName="Karthik")

        query = build_query_for_strategy(strategy, customer)

        assert "Karthik~1" in query

    def test_ssn_undashed_input_normalized(self):
        """SSN stored undashed should still produce both dashed and undashed forms."""
        from app.services.search_service import build_query_for_strategy, Strategy

        strategy = Strategy(name="test", description="", fields=["SSN"])
        customer = FakeCustomer(SSN="343434343")  # stored undashed

        query = build_query_for_strategy(strategy, customer)

        assert '"343-43-4343"' in query
        assert '"343434343"' in query


# ===========================================================================
# TEST CLASS: Load Strategies from YAML
# ===========================================================================

class TestLoadStrategies:
    """Tests for load_strategies (spec: Strategy set configuration via YAML file)."""

    def test_load_default_strategy_set(self):
        """Scenario: Load default strategy set.

        WHEN the system reads strategies.yaml
        THEN all strategies are loaded and available.
        """
        from app.services.search_service import load_strategies

        yaml_path = "C:/Users/karth/pwc/breach-search/strategies.yaml"
        strategies = load_strategies(yaml_path)

        assert len(strategies) == 3
        names = [s.name for s in strategies]
        assert "fullname_ssn" in names
        assert "lastname_dob" in names
        assert "unique_identifiers" in names

    def test_default_strategy_fields(self):
        """Scenario: Default strategy set contents.

        WHEN the system ships with the default strategies.yaml
        THEN it contains the three default strategies with correct fields.
        """
        from app.services.search_service import load_strategies

        yaml_path = "C:/Users/karth/pwc/breach-search/strategies.yaml"
        strategies = load_strategies(yaml_path)

        by_name = {s.name: s for s in strategies}

        assert by_name["fullname_ssn"].fields == ["Fullname", "SSN"]
        assert by_name["lastname_dob"].fields == ["LastName", "DOB"]
        assert by_name["unique_identifiers"].fields == ["SSN", "DriversLicense"]

    def test_custom_strategy_override(self):
        """Scenario: Custom strategy override.

        WHEN the user edits strategies.yaml to contain a single strategy
        THEN the system uses only that one strategy.
        """
        from app.services.search_service import load_strategies

        yaml_content = {
            "strategies": [
                {"name": "ssn_only", "description": "SSN only", "fields": ["SSN"]}
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = f.name

        try:
            strategies = load_strategies(temp_path)
            assert len(strategies) == 1
            assert strategies[0].name == "ssn_only"
            assert strategies[0].fields == ["SSN"]
        finally:
            os.unlink(temp_path)

    def test_missing_yaml_raises_error(self):
        """Scenario: Invalid strategy file — YAML missing.

        WHEN strategies.yaml is missing
        THEN raises a clear error with the file path.
        """
        from app.services.search_service import load_strategies

        with pytest.raises(Exception, match="strategies.yaml"):
            load_strategies("/nonexistent/path/strategies.yaml")

    def test_invalid_yaml_raises_error(self):
        """Scenario: Invalid strategy file — YAML malformed.

        WHEN strategies.yaml contains invalid YAML
        THEN raises a clear error.
        """
        from app.services.search_service import load_strategies

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write(": invalid: yaml: content: {{{")
            temp_path = f.name

        try:
            with pytest.raises(Exception):
                load_strategies(temp_path)
        finally:
            os.unlink(temp_path)

    def test_invalid_field_name_raises_validation_error(self):
        """Scenario: Strategy references invalid field name.

        WHEN a strategy contains fields: ["Fullname", "InvalidField"]
        THEN raises a validation error listing the invalid field name.
        """
        from app.services.search_service import load_strategies

        yaml_content = {
            "strategies": [
                {
                    "name": "bad_strategy",
                    "description": "Uses invalid field",
                    "fields": ["Fullname", "InvalidField"]
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = f.name

        try:
            with pytest.raises(ValueError, match="InvalidField"):
                load_strategies(temp_path)
        finally:
            os.unlink(temp_path)

    def test_strategy_object_has_correct_attributes(self):
        """Strategy objects returned from load_strategies have name, description, fields."""
        from app.services.search_service import load_strategies, Strategy

        yaml_content = {
            "strategies": [
                {
                    "name": "test_strat",
                    "description": "A test strategy",
                    "fields": ["SSN"]
                }
            ]
        }

        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(yaml_content, f)
            temp_path = f.name

        try:
            strategies = load_strategies(temp_path)
            s = strategies[0]
            assert isinstance(s, Strategy)
            assert s.name == "test_strat"
            assert s.description == "A test strategy"
            assert s.fields == ["SSN"]
        finally:
            os.unlink(temp_path)


# ===========================================================================
# TEST CLASS: Execute Strategy Query
# ===========================================================================

class TestExecuteStrategyQuery:
    """Tests for execute_strategy_query (spec: Azure AI Search query execution per strategy)."""

    def test_execute_strategy_query_correct_params(self):
        """Scenario: Execute strategy query with correct Azure AI Search parameters.

        WHEN the system executes a strategy query
        THEN Azure AI Search is called with queryType=full, searchMode=any,
             searchFields=[content, content_phonetic, content_lowercase],
             scoringProfile=pii_boost, top=100.
        """
        from app.services.search_service import execute_strategy_query

        mock_client = MagicMock()
        result1 = FakeSearchResult("md5abc123", 10.5, "case1/file.txt")
        result2 = FakeSearchResult("md5def456", 8.3, "case1/file2.txt")
        mock_client.search.return_value = [result1, result2]

        results = execute_strategy_query(
            mock_client,
            'Karthik~1 Chekuri~1 OR "343-43-4343" OR "343434343"'
        )

        mock_client.search.assert_called_once()
        call_kwargs = mock_client.search.call_args
        assert call_kwargs.kwargs["query_type"] == "full"
        assert call_kwargs.kwargs["search_mode"] == "any"
        assert call_kwargs.kwargs["search_fields"] == [
            "content", "content_phonetic", "content_lowercase"
        ]
        assert call_kwargs.kwargs["scoring_profile"] == "pii_boost"
        assert call_kwargs.kwargs["top"] == 100

    def test_execute_strategy_query_returns_md5_keyed_results(self):
        """Results from execute_strategy_query are keyed by MD5."""
        from app.services.search_service import execute_strategy_query

        mock_client = MagicMock()
        result1 = FakeSearchResult("md5abc123", 10.5, "case1/file.txt")
        mock_client.search.return_value = [result1]

        results = execute_strategy_query(mock_client, "some query")

        assert len(results) == 1
        assert results[0]["md5"] == "md5abc123"
        assert results[0]["search_score"] == 10.5
        assert results[0]["file_path"] == "case1/file.txt"

    def test_execute_strategy_query_empty_results(self):
        """When no matching files, returns empty list."""
        from app.services.search_service import execute_strategy_query

        mock_client = MagicMock()
        mock_client.search.return_value = []

        results = execute_strategy_query(mock_client, "some query")

        assert results == []

    def test_execute_strategy_query_multiple_results(self):
        """Returns all results up to 100."""
        from app.services.search_service import execute_strategy_query

        mock_client = MagicMock()
        mock_results = [FakeSearchResult(f"md5{i:03d}", float(i), f"file{i}.txt") for i in range(5)]
        mock_client.search.return_value = mock_results

        results = execute_strategy_query(mock_client, "Karthik~1")

        assert len(results) == 5
        assert all("md5" in r for r in results)
        assert all("search_score" in r for r in results)


# ===========================================================================
# TEST CLASS: search_customer — Multi-Strategy Union
# ===========================================================================

class TestSearchCustomer:
    """Tests for search_customer (spec: Multiple strategies produce union of candidates)."""

    def test_single_strategy_execution(self):
        """Test single strategy execution returns correct candidates.

        GIVEN one strategy that returns two files
        WHEN search_customer is called
        THEN both files are in the candidate set.
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"])
        ]
        customer = FakeCustomer()

        # Strategy returns 2 results
        mock_client.search.return_value = [
            FakeSearchResult("md5aaa", 12.5, "file_a.txt"),
            FakeSearchResult("md5bbb", 8.0, "file_b.txt"),
        ]

        candidates = search_customer(mock_client, customer, strategies)

        md5s = {c["md5"] for c in candidates}
        assert "md5aaa" in md5s
        assert "md5bbb" in md5s
        assert len(candidates) == 2

    def test_multi_strategy_union_dedup_by_md5(self):
        """Scenario: Three strategies produce overlapping results.

        GIVEN three strategies
        WHEN strategy 1 returns [file_a, file_b], strategy 2 returns [file_a, file_d],
             strategy 3 returns [file_a, file_e]
        THEN union of candidates is [file_a, file_b, file_d, file_e] (deduplicated by MD5).
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="lastname_dob", description="", fields=["LastName", "DOB"]),
            Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
        ]
        customer = FakeCustomer()

        # Set up side_effect: each strategy call returns different results
        mock_client.search.side_effect = [
            # Strategy 1: file_a, file_b
            [FakeSearchResult("md5_a", 12.5, "file_a.txt"),
             FakeSearchResult("md5_b", 9.0, "file_b.txt")],
            # Strategy 2: file_a, file_d
            [FakeSearchResult("md5_a", 7.0, "file_a.txt"),
             FakeSearchResult("md5_d", 5.0, "file_d.txt")],
            # Strategy 3: file_a, file_e
            [FakeSearchResult("md5_a", 6.0, "file_a.txt"),
             FakeSearchResult("md5_e", 4.0, "file_e.txt")],
        ]

        candidates = search_customer(mock_client, customer, strategies)

        md5s = {c["md5"] for c in candidates}
        assert md5s == {"md5_a", "md5_b", "md5_d", "md5_e"}
        assert len(candidates) == 4  # deduplicated

    def test_highest_score_wins_across_strategies(self):
        """Scenario: File found by multiple strategies records highest score.

        WHEN file_a is returned by fullname_ssn (score 12.5) and unique_identifiers (score 9.0)
        THEN result for file_a has azure_search_score=12.5 (highest score).
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
        ]
        customer = FakeCustomer()

        mock_client.search.side_effect = [
            # Strategy 1: file_a with score 12.5
            [FakeSearchResult("md5_a", 12.5, "file_a.txt")],
            # Strategy 2: file_a with score 9.0 (lower)
            [FakeSearchResult("md5_a", 9.0, "file_a.txt")],
        ]

        candidates = search_customer(mock_client, customer, strategies)

        assert len(candidates) == 1
        file_a = candidates[0]
        assert file_a["md5"] == "md5_a"
        assert file_a["azure_search_score"] == 12.5

    def test_strategy_that_found_it_is_first_strategy(self):
        """Scenario: File found by multiple strategies records first match.

        WHEN file_a is returned by both fullname_ssn and unique_identifiers
        THEN strategy_that_found_it is "fullname_ssn" (first strategy that found it).
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
        ]
        customer = FakeCustomer()

        mock_client.search.side_effect = [
            # Strategy 1 finds file_a first
            [FakeSearchResult("md5_a", 12.5, "file_a.txt")],
            # Strategy 2 also finds file_a
            [FakeSearchResult("md5_a", 9.0, "file_a.txt")],
        ]

        candidates = search_customer(mock_client, customer, strategies)

        assert len(candidates) == 1
        file_a = candidates[0]
        assert file_a["strategy_that_found_it"] == "fullname_ssn"

    def test_empty_strategy_results_not_error(self):
        """Scenario: Strategies that return no results.

        WHEN strategy lastname_dob returns zero results for a customer
        THEN the other strategies' results still form the candidate set.
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="lastname_dob", description="", fields=["LastName", "DOB"]),
        ]
        customer = FakeCustomer()

        mock_client.search.side_effect = [
            # Strategy 1 returns results
            [FakeSearchResult("md5_a", 10.0, "file_a.txt")],
            # Strategy 2 returns nothing
            [],
        ]

        candidates = search_customer(mock_client, customer, strategies)

        assert len(candidates) == 1
        assert candidates[0]["md5"] == "md5_a"

    def test_all_strategies_empty_returns_empty_list(self):
        """Scenario: No matches from any strategy.

        WHEN all strategies return zero results for a customer
        THEN candidate list is empty.
        """
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="lastname_dob", description="", fields=["LastName", "DOB"]),
            Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
        ]
        customer = FakeCustomer()

        mock_client.search.side_effect = [[], [], []]

        candidates = search_customer(mock_client, customer, strategies)

        assert candidates == []

    def test_azure_search_called_once_per_strategy(self):
        """Azure AI Search is called exactly once per strategy."""
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
            Strategy(name="lastname_dob", description="", fields=["LastName", "DOB"]),
            Strategy(name="unique_identifiers", description="", fields=["SSN", "DriversLicense"]),
        ]
        customer = FakeCustomer()
        mock_client.search.return_value = []

        search_customer(mock_client, customer, strategies)

        assert mock_client.search.call_count == 3

    def test_candidate_includes_file_path(self):
        """Each candidate in the result set includes the file_path."""
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="fullname_ssn", description="", fields=["Fullname", "SSN"]),
        ]
        customer = FakeCustomer()

        mock_client.search.return_value = [
            FakeSearchResult("md5abc", 10.0, "case1/payroll.xlsx"),
        ]

        candidates = search_customer(mock_client, customer, strategies)

        assert len(candidates) == 1
        assert candidates[0]["file_path"] == "case1/payroll.xlsx"

    def test_null_fields_strategy_skipped(self):
        """When strategy query returns None (all fields null), Azure Search not called for that strategy."""
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        # Strategy using DriversLicense only, but customer has no DriversLicense
        strategies = [
            Strategy(name="dl_only", description="", fields=["DriversLicense"]),
        ]
        customer = FakeCustomer(DriversLicense=None)

        candidates = search_customer(mock_client, customer, strategies)

        # Azure Search should NOT be called for a null-query strategy
        mock_client.search.assert_not_called()
        assert candidates == []

    def test_unique_files_even_with_different_scores(self):
        """When same MD5 returned with different scores, highest score is kept."""
        from app.services.search_service import search_customer, Strategy

        mock_client = MagicMock()
        strategies = [
            Strategy(name="s1", description="", fields=["Fullname"]),
            Strategy(name="s2", description="", fields=["SSN"]),
            Strategy(name="s3", description="", fields=["DriversLicense"]),
        ]
        customer = FakeCustomer()

        mock_client.search.side_effect = [
            [FakeSearchResult("md5_x", 5.0, "file_x.txt")],
            [FakeSearchResult("md5_x", 15.0, "file_x.txt")],  # highest
            [FakeSearchResult("md5_x", 10.0, "file_x.txt")],
        ]

        candidates = search_customer(mock_client, customer, strategies)

        assert len(candidates) == 1
        assert candidates[0]["azure_search_score"] == 15.0

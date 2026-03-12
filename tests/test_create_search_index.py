"""
Tests for scripts/create_search_index.py — Azure AI Search index setup script.

TDD: These tests are written BEFORE the production code. They define the
expected contract for the index creation script.

Phase 2.2: Azure AI Search Index Setup Script

Scenarios tested:
- Index is created with correct name ('breach-file-index')
- Phonetic analyzer is correctly configured (Double Metaphone, replace=false)
- Name analyzer is correctly configured (standard tokenizer, lowercase, asciifolding)
- Three content fields use the correct analyzers
- Metadata fields are present with correct types
- Scoring profile 'pii_boost' has correct weights
- Script reads config from environment variables
"""
import logging
from unittest.mock import MagicMock, patch, call

import pytest
from azure.search.documents.indexes.models import (
    SearchIndex,
    SearchField,
    SearchFieldDataType,
    CustomAnalyzer,
    PhoneticTokenFilter,
    PhoneticEncoder,
    ScoringProfile,
    TextWeights,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def env_vars(monkeypatch):
    """Set required environment variables for the script."""
    monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://test-search.search.windows.net")
    monkeypatch.setenv("AZURE_SEARCH_KEY", "test-admin-key-abc123")
    monkeypatch.setenv("AZURE_SEARCH_INDEX", "breach-file-index")


@pytest.fixture
def mock_index_client():
    """Return a mocked SearchIndexClient."""
    with patch("scripts.create_search_index.SearchIndexClient") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_credential():
    """Return a mocked AzureKeyCredential."""
    with patch("scripts.create_search_index.AzureKeyCredential") as MockCred:
        yield MockCred


# ---------------------------------------------------------------------------
# Helper: build the index definition by calling the function under test
# ---------------------------------------------------------------------------

def _build_index():
    """Import and call build_index_definition() to get the SearchIndex object."""
    from scripts.create_search_index import build_index_definition
    return build_index_definition("breach-file-index")


def _get_field(index: SearchIndex, name: str):
    """Find a field by name in the index definition."""
    for field in index.fields:
        if field.name == name:
            return field
    raise AssertionError(f"Field '{name}' not found in index. Fields: {[f.name for f in index.fields]}")


def _get_analyzer(index: SearchIndex, name: str):
    """Find a custom analyzer by name in the index definition."""
    for analyzer in (index.analyzers or []):
        if analyzer.name == name:
            return analyzer
    raise AssertionError(f"Analyzer '{name}' not found in index. Analyzers: {[a.name for a in (index.analyzers or [])]}")


def _get_token_filter(index: SearchIndex, name: str):
    """Find a token filter by name in the index definition."""
    for tf in (index.token_filters or []):
        if tf.name == name:
            return tf
    raise AssertionError(f"Token filter '{name}' not found. Filters: {[t.name for t in (index.token_filters or [])]}")


# ===========================================================================
# Test Class: Index Definition Structure
# ===========================================================================

class TestIndexDefinitionStructure:
    """The build_index_definition function must return a properly structured SearchIndex."""

    def test_returns_search_index_instance(self):
        """build_index_definition must return a SearchIndex object."""
        index = _build_index()
        assert isinstance(index, SearchIndex)

    def test_index_name_is_breach_file_index(self):
        """The index must be named 'breach-file-index'."""
        index = _build_index()
        assert index.name == "breach-file-index"

    def test_index_name_uses_provided_name(self):
        """The index name must match the parameter passed to build_index_definition."""
        from scripts.create_search_index import build_index_definition
        index = build_index_definition("custom-index-name")
        assert index.name == "custom-index-name"


# ===========================================================================
# Test Class: Custom Analyzers
# ===========================================================================

class TestPhoneticAnalyzer:
    """The phonetic_analyzer must use Double Metaphone with replace=false."""

    def test_phonetic_analyzer_exists(self):
        """Index must define a custom analyzer named 'phonetic_analyzer'."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert analyzer is not None

    def test_phonetic_analyzer_is_custom_analyzer(self):
        """phonetic_analyzer must be a CustomAnalyzer instance."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert isinstance(analyzer, CustomAnalyzer)

    def test_phonetic_analyzer_uses_standard_tokenizer(self):
        """phonetic_analyzer must use the 'standard_v2' tokenizer."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert analyzer.tokenizer_name == "standard_v2"

    def test_phonetic_analyzer_has_lowercase_filter(self):
        """phonetic_analyzer must include the 'lowercase' token filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert "lowercase" in analyzer.token_filters

    def test_phonetic_analyzer_has_asciifolding_filter(self):
        """phonetic_analyzer must include the 'asciifolding' token filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert "asciifolding" in analyzer.token_filters

    def test_phonetic_analyzer_has_phonetic_filter(self):
        """phonetic_analyzer must include the custom 'double_metaphone_filter'."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert "double_metaphone_filter" in analyzer.token_filters

    def test_phonetic_analyzer_filter_order(self):
        """phonetic_analyzer filters must be in order: lowercase, asciifolding, double_metaphone_filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        expected_order = ["lowercase", "asciifolding", "double_metaphone_filter"]
        assert analyzer.token_filters == expected_order

    def test_double_metaphone_filter_exists(self):
        """Index must define a PhoneticTokenFilter named 'double_metaphone_filter'."""
        index = _build_index()
        tf = _get_token_filter(index, "double_metaphone_filter")
        assert isinstance(tf, PhoneticTokenFilter)

    def test_double_metaphone_filter_uses_double_metaphone(self):
        """double_metaphone_filter must use the 'doubleMetaphone' encoder."""
        index = _build_index()
        tf = _get_token_filter(index, "double_metaphone_filter")
        assert tf.encoder == PhoneticEncoder.DOUBLE_METAPHONE

    def test_double_metaphone_filter_replace_is_false(self):
        """double_metaphone_filter must set replace_original_tokens=False to keep original tokens."""
        index = _build_index()
        tf = _get_token_filter(index, "double_metaphone_filter")
        assert tf.replace_original_tokens is False


class TestNameAnalyzer:
    """The name_analyzer must have standard tokenizer, lowercase, and asciifolding."""

    def test_name_analyzer_exists(self):
        """Index must define a custom analyzer named 'name_analyzer'."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert analyzer is not None

    def test_name_analyzer_is_custom_analyzer(self):
        """name_analyzer must be a CustomAnalyzer instance."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert isinstance(analyzer, CustomAnalyzer)

    def test_name_analyzer_uses_standard_tokenizer(self):
        """name_analyzer must use the 'standard_v2' tokenizer."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert analyzer.tokenizer_name == "standard_v2"

    def test_name_analyzer_has_lowercase_filter(self):
        """name_analyzer must include the 'lowercase' token filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert "lowercase" in analyzer.token_filters

    def test_name_analyzer_has_asciifolding_filter(self):
        """name_analyzer must include the 'asciifolding' token filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert "asciifolding" in analyzer.token_filters

    def test_name_analyzer_filter_order(self):
        """name_analyzer filters must be in order: lowercase, asciifolding."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        expected_order = ["lowercase", "asciifolding"]
        assert analyzer.token_filters == expected_order


# ===========================================================================
# Test Class: Content Fields
# ===========================================================================

class TestContentFields:
    """Three content fields must exist, each with the correct analyzer."""

    def test_content_field_exists(self):
        """Index must have a 'content' field."""
        index = _build_index()
        field = _get_field(index, "content")
        assert field is not None

    def test_content_field_is_searchable(self):
        """content field must be searchable."""
        index = _build_index()
        field = _get_field(index, "content")
        assert field.searchable is True

    def test_content_field_uses_standard_lucene_analyzer(self):
        """content field must use 'standard.lucene' analyzer."""
        index = _build_index()
        field = _get_field(index, "content")
        assert field.analyzer_name == "standard.lucene"

    def test_content_phonetic_field_exists(self):
        """Index must have a 'content_phonetic' field."""
        index = _build_index()
        field = _get_field(index, "content_phonetic")
        assert field is not None

    def test_content_phonetic_field_is_searchable(self):
        """content_phonetic field must be searchable."""
        index = _build_index()
        field = _get_field(index, "content_phonetic")
        assert field.searchable is True

    def test_content_phonetic_field_uses_phonetic_analyzer(self):
        """content_phonetic field must use the custom 'phonetic_analyzer'."""
        index = _build_index()
        field = _get_field(index, "content_phonetic")
        assert field.analyzer_name == "phonetic_analyzer"

    def test_content_lowercase_field_exists(self):
        """Index must have a 'content_lowercase' field."""
        index = _build_index()
        field = _get_field(index, "content_lowercase")
        assert field is not None

    def test_content_lowercase_field_is_searchable(self):
        """content_lowercase field must be searchable."""
        index = _build_index()
        field = _get_field(index, "content_lowercase")
        assert field.searchable is True

    def test_content_lowercase_field_uses_name_analyzer(self):
        """content_lowercase field must use the custom 'name_analyzer'."""
        index = _build_index()
        field = _get_field(index, "content_lowercase")
        assert field.analyzer_name == "name_analyzer"

    def test_content_fields_are_edm_string_type(self):
        """All three content fields must be of type Edm.String."""
        index = _build_index()
        for field_name in ["content", "content_phonetic", "content_lowercase"]:
            field = _get_field(index, field_name)
            assert field.type == SearchFieldDataType.String, (
                f"{field_name} should be Edm.String, got {field.type}"
            )


# ===========================================================================
# Test Class: Metadata Fields
# ===========================================================================

class TestMetadataFields:
    """V2 metadata fields: id (key, MD5), md5, file_path."""

    def test_id_field_exists_and_is_key(self):
        """Index must have an 'id' field that is the document key (MD5 hash)."""
        index = _build_index()
        field = _get_field(index, "id")
        assert field.key is True

    def test_id_field_is_string(self):
        """id field must be of type Edm.String."""
        index = _build_index()
        field = _get_field(index, "id")
        assert field.type == SearchFieldDataType.String

    def test_md5_field_exists(self):
        """V2: Index must have a 'md5' field."""
        index = _build_index()
        field = _get_field(index, "md5")
        assert field.type == SearchFieldDataType.String

    def test_md5_field_is_filterable(self):
        """md5 must be filterable for single-file lookups."""
        index = _build_index()
        field = _get_field(index, "md5")
        assert field.filterable is True

    def test_file_path_field_exists(self):
        """Index must have a 'file_path' field."""
        index = _build_index()
        field = _get_field(index, "file_path")
        assert field.type == SearchFieldDataType.String

    # V1 fields removed in V2 — tests skipped to document intentional removal
    @pytest.mark.skip(reason="V1 field file_guid removed in V2 — id is now MD5 hash")
    def test_file_guid_field_exists(self):
        """V1: file_guid field was replaced by id=MD5."""
        pass

    @pytest.mark.skip(reason="V1 field file_name removed in V2")
    def test_file_name_field_exists(self):
        """V1: file_name field removed in V2 (inferred from file_path)."""
        pass

    @pytest.mark.skip(reason="V1 field file_extension removed in V2 (inferred from file_path at runtime)")
    def test_file_extension_field_exists(self):
        """V1: file_extension field removed in V2."""
        pass

    @pytest.mark.skip(reason="V1 field case_name removed in V2")
    def test_case_name_field_exists(self):
        """V1: case_name field removed in V2 (no case filtering needed)."""
        pass


# ===========================================================================
# Test Class: Scoring Profile
# ===========================================================================

class TestScoringProfile:
    """The pii_boost scoring profile must weight content fields correctly."""

    def test_scoring_profile_exists(self):
        """Index must have a scoring profile named 'pii_boost'."""
        index = _build_index()
        profiles = index.scoring_profiles or []
        names = [p.name for p in profiles]
        assert "pii_boost" in names

    def test_scoring_profile_is_scoring_profile_instance(self):
        """pii_boost must be a ScoringProfile instance."""
        index = _build_index()
        profile = None
        for p in (index.scoring_profiles or []):
            if p.name == "pii_boost":
                profile = p
                break
        assert isinstance(profile, ScoringProfile)

    def test_content_weight_is_3(self):
        """pii_boost must weight 'content' at 3.0."""
        index = _build_index()
        profile = [p for p in index.scoring_profiles if p.name == "pii_boost"][0]
        assert profile.text_weights.weights["content"] == 3.0

    def test_content_lowercase_weight_is_2(self):
        """pii_boost must weight 'content_lowercase' at 2.0."""
        index = _build_index()
        profile = [p for p in index.scoring_profiles if p.name == "pii_boost"][0]
        assert profile.text_weights.weights["content_lowercase"] == 2.0

    def test_content_phonetic_weight_is_1_5(self):
        """pii_boost must weight 'content_phonetic' at 1.5."""
        index = _build_index()
        profile = [p for p in index.scoring_profiles if p.name == "pii_boost"][0]
        assert profile.text_weights.weights["content_phonetic"] == 1.5

    def test_default_scoring_profile_is_pii_boost(self):
        """The index default_scoring_profile should be 'pii_boost'."""
        index = _build_index()
        assert index.default_scoring_profile == "pii_boost"


# ===========================================================================
# Test Class: Index Creation (create_or_update_index)
# ===========================================================================

class TestCreateIndex:
    """The create_index function must call the Azure SDK to create the index."""

    def test_create_index_calls_create_or_update(self, env_vars, mock_credential, mock_index_client):
        """create_index must call client.create_or_update_index with the index definition."""
        from scripts.create_search_index import create_index

        create_index()

        mock_index_client.create_or_update_index.assert_called_once()
        args, kwargs = mock_index_client.create_or_update_index.call_args
        index_arg = args[0] if args else kwargs.get("index")
        assert isinstance(index_arg, SearchIndex)
        assert index_arg.name == "breach-file-index"

    def test_create_index_uses_endpoint_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_index must read AZURE_SEARCH_ENDPOINT from env."""
        from scripts.create_search_index import create_index

        create_index()

        from scripts.create_search_index import AzureKeyCredential
        from scripts.create_search_index import SearchIndexClient
        SearchIndexClient.assert_called_once()
        call_args = SearchIndexClient.call_args
        assert call_args[1].get("endpoint") == "https://test-search.search.windows.net" or \
               (call_args[0] and call_args[0][0] == "https://test-search.search.windows.net")

    def test_create_index_uses_key_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_index must use AZURE_SEARCH_KEY to create AzureKeyCredential."""
        from scripts.create_search_index import create_index

        create_index()

        from scripts.create_search_index import AzureKeyCredential
        AzureKeyCredential.assert_called_once_with("test-admin-key-abc123")

    def test_create_index_uses_index_name_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_index must use AZURE_SEARCH_INDEX from env as the index name."""
        from scripts.create_search_index import create_index

        create_index()

        args, kwargs = mock_index_client.create_or_update_index.call_args
        index_arg = args[0] if args else kwargs.get("index")
        assert index_arg.name == "breach-file-index"

    def test_create_index_logs_success(self, env_vars, mock_credential, mock_index_client, caplog):
        """create_index must log a success message after creating the index."""
        from scripts.create_search_index import create_index

        mock_index_client.create_or_update_index.return_value = MagicMock(name="breach-file-index")

        with caplog.at_level(logging.INFO):
            create_index()

        assert any("breach-file-index" in record.message for record in caplog.records)


# ===========================================================================
# Test Class: Complete Field List
# ===========================================================================

class TestCompleteFieldList:
    """V2: The index must contain all required V2 fields — no more, no less."""

    # V2 field set: id (MD5 key), md5, file_path + 3 content fields
    EXPECTED_FIELDS = {
        "id", "md5", "file_path",
        "content", "content_phonetic", "content_lowercase",
    }

    def test_all_expected_fields_present(self):
        """Index must contain all 6 expected V2 fields."""
        index = _build_index()
        actual_fields = {f.name for f in index.fields}
        assert self.EXPECTED_FIELDS.issubset(actual_fields), (
            f"Missing fields: {self.EXPECTED_FIELDS - actual_fields}"
        )

    def test_no_unexpected_fields(self):
        """Index must not contain fields beyond the 6 expected V2 ones."""
        index = _build_index()
        actual_fields = {f.name for f in index.fields}
        unexpected = actual_fields - self.EXPECTED_FIELDS
        assert not unexpected, f"Unexpected fields: {unexpected}"

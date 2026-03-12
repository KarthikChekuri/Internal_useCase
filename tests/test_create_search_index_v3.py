"""
Tests for scripts/create_search_index_v3.py — Azure AI Search index setup script (V3).

TDD: These tests are written BEFORE the production code. They define the
expected contract for the V3 index creation script.

Phase V3-1.3: V3 Search Index Script

Scenarios tested:
- Index is created with correct name ('breach-file-index-v3')
- All V2 content fields are present with correct analyzers
- All V2 metadata fields are present (id, md5, file_path)
- V3 PII metadata fields: has_ssn, has_name, has_dob, has_address, has_phone (Boolean, filterable)
- pii_types field: Collection(String), filterable
- pii_entity_count field: Int32, filterable
- Phonetic and name analyzers same config as V2
- Scoring profile 'pii_boost' has correct weights (same as V2)
- create_v3_index reads config from environment variables
"""
import logging
from unittest.mock import MagicMock, patch

import pytest
from azure.search.documents.indexes.models import (
    CustomAnalyzer,
    PhoneticEncoder,
    PhoneticTokenFilter,
    ScoringProfile,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
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
    monkeypatch.setenv("AZURE_SEARCH_INDEX_V3", "breach-file-index-v3")


@pytest.fixture
def mock_index_client():
    """Return a mocked SearchIndexClient."""
    with patch("scripts.create_search_index_v3.SearchIndexClient") as MockClient:
        mock_instance = MagicMock()
        MockClient.return_value = mock_instance
        yield mock_instance


@pytest.fixture
def mock_credential():
    """Return a mocked AzureKeyCredential."""
    with patch("scripts.create_search_index_v3.AzureKeyCredential") as MockCred:
        yield MockCred


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _build_index():
    """Import and call build_v3_index_definition() to get the SearchIndex object."""
    from scripts.create_search_index_v3 import build_v3_index_definition
    return build_v3_index_definition("breach-file-index-v3")


def _get_field(index: SearchIndex, name: str):
    """Find a field by name in the index definition."""
    for field in index.fields:
        if field.name == name:
            return field
    raise AssertionError(
        f"Field '{name}' not found in index. Fields: {[f.name for f in index.fields]}"
    )


def _get_analyzer(index: SearchIndex, name: str):
    """Find a custom analyzer by name in the index definition."""
    for analyzer in (index.analyzers or []):
        if analyzer.name == name:
            return analyzer
    raise AssertionError(
        f"Analyzer '{name}' not found. Analyzers: {[a.name for a in (index.analyzers or [])]}"
    )


def _get_token_filter(index: SearchIndex, name: str):
    """Find a token filter by name in the index definition."""
    for tf in (index.token_filters or []):
        if tf.name == name:
            return tf
    raise AssertionError(
        f"Token filter '{name}' not found. Filters: {[t.name for t in (index.token_filters or [])]}"
    )


# ===========================================================================
# Test Class: Index Definition Structure
# ===========================================================================

class TestIndexDefinitionStructure:
    """build_v3_index_definition must return a properly structured SearchIndex."""

    def test_returns_search_index_instance(self):
        """build_v3_index_definition must return a SearchIndex object."""
        index = _build_index()
        assert isinstance(index, SearchIndex)

    def test_index_name_is_breach_file_index_v3(self):
        """The index must be named 'breach-file-index-v3' by default."""
        index = _build_index()
        assert index.name == "breach-file-index-v3"

    def test_index_name_uses_provided_name(self):
        """The index name must match the parameter passed to build_v3_index_definition."""
        from scripts.create_search_index_v3 import build_v3_index_definition
        index = build_v3_index_definition("custom-v3-index")
        assert index.name == "custom-v3-index"


# ===========================================================================
# Test Class: V2 Metadata Fields (preserved in V3)
# ===========================================================================

class TestV2MetadataFields:
    """V2 metadata fields must all be present in V3 with the same config."""

    def test_id_field_exists_and_is_key(self):
        """Index must have an 'id' field that is the document key."""
        index = _build_index()
        field = _get_field(index, "id")
        assert field.key is True

    def test_id_field_is_string(self):
        """id field must be of type Edm.String."""
        index = _build_index()
        field = _get_field(index, "id")
        assert field.type == SearchFieldDataType.String

    def test_id_field_is_filterable(self):
        """id field must be filterable."""
        index = _build_index()
        field = _get_field(index, "id")
        assert field.filterable is True

    def test_md5_field_exists(self):
        """Index must have a 'md5' field."""
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

    def test_file_path_not_filterable(self):
        """file_path must not be filterable (V2 spec)."""
        index = _build_index()
        field = _get_field(index, "file_path")
        assert not field.filterable


# ===========================================================================
# Test Class: V2 Content Fields (preserved in V3)
# ===========================================================================

class TestV2ContentFields:
    """All three V2 content fields must be present with correct analyzers."""

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
# Test Class: V3 PII Boolean Fields
# ===========================================================================

class TestV3PIIBooleanFields:
    """V3 adds 5 Boolean PII presence fields, all filterable and non-searchable."""

    PII_BOOL_FIELDS = ["has_ssn", "has_name", "has_dob", "has_address", "has_phone"]

    def test_has_ssn_field_exists(self):
        """Index must have a 'has_ssn' field."""
        index = _build_index()
        field = _get_field(index, "has_ssn")
        assert field is not None

    def test_has_name_field_exists(self):
        """Index must have a 'has_name' field."""
        index = _build_index()
        field = _get_field(index, "has_name")
        assert field is not None

    def test_has_dob_field_exists(self):
        """Index must have a 'has_dob' field."""
        index = _build_index()
        field = _get_field(index, "has_dob")
        assert field is not None

    def test_has_address_field_exists(self):
        """Index must have a 'has_address' field."""
        index = _build_index()
        field = _get_field(index, "has_address")
        assert field is not None

    def test_has_phone_field_exists(self):
        """Index must have a 'has_phone' field."""
        index = _build_index()
        field = _get_field(index, "has_phone")
        assert field is not None

    def test_all_pii_bool_fields_are_boolean_type(self):
        """All 5 PII presence fields must be of type Edm.Boolean."""
        index = _build_index()
        for field_name in self.PII_BOOL_FIELDS:
            field = _get_field(index, field_name)
            assert field.type == SearchFieldDataType.Boolean, (
                f"{field_name} should be Edm.Boolean, got {field.type}"
            )

    def test_all_pii_bool_fields_are_filterable(self):
        """All 5 PII presence fields must be filterable for query-time filtering."""
        index = _build_index()
        for field_name in self.PII_BOOL_FIELDS:
            field = _get_field(index, field_name)
            assert field.filterable is True, (
                f"{field_name} should be filterable"
            )

    def test_all_pii_bool_fields_are_not_searchable(self):
        """PII presence fields are metadata flags and must not be searchable."""
        index = _build_index()
        for field_name in self.PII_BOOL_FIELDS:
            field = _get_field(index, field_name)
            assert not field.searchable, (
                f"{field_name} should not be searchable"
            )


# ===========================================================================
# Test Class: V3 PII Collection and Count Fields
# ===========================================================================

class TestV3PIICollectionAndCountFields:
    """V3 adds pii_types (Collection(String)) and pii_entity_count (Int32)."""

    def test_pii_types_field_exists(self):
        """Index must have a 'pii_types' field."""
        index = _build_index()
        field = _get_field(index, "pii_types")
        assert field is not None

    def test_pii_types_field_is_collection_string(self):
        """pii_types must be a Collection(Edm.String)."""
        index = _build_index()
        field = _get_field(index, "pii_types")
        assert field.type == SearchFieldDataType.Collection(SearchFieldDataType.String)

    def test_pii_types_field_is_filterable(self):
        """pii_types must be filterable to query documents containing specific PII categories."""
        index = _build_index()
        field = _get_field(index, "pii_types")
        assert field.filterable is True

    def test_pii_types_field_is_not_searchable(self):
        """pii_types is a metadata field and must not be searchable."""
        index = _build_index()
        field = _get_field(index, "pii_types")
        assert not field.searchable

    def test_pii_entity_count_field_exists(self):
        """Index must have a 'pii_entity_count' field."""
        index = _build_index()
        field = _get_field(index, "pii_entity_count")
        assert field is not None

    def test_pii_entity_count_field_is_int32(self):
        """pii_entity_count must be of type Edm.Int32."""
        index = _build_index()
        field = _get_field(index, "pii_entity_count")
        assert field.type == SearchFieldDataType.Int32

    def test_pii_entity_count_field_is_filterable(self):
        """pii_entity_count must be filterable to filter by minimum PII count."""
        index = _build_index()
        field = _get_field(index, "pii_entity_count")
        assert field.filterable is True

    def test_pii_entity_count_field_is_not_searchable(self):
        """pii_entity_count is a metadata count and must not be searchable."""
        index = _build_index()
        field = _get_field(index, "pii_entity_count")
        assert not field.searchable


# ===========================================================================
# Test Class: Custom Analyzers (same as V2)
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

    def test_phonetic_analyzer_filter_order(self):
        """phonetic_analyzer filters must be in order: lowercase, asciifolding, double_metaphone_filter."""
        index = _build_index()
        analyzer = _get_analyzer(index, "phonetic_analyzer")
        assert analyzer.token_filters == ["lowercase", "asciifolding", "double_metaphone_filter"]

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
        """double_metaphone_filter must set replace_original_tokens=False."""
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

    def test_name_analyzer_filter_order(self):
        """name_analyzer filters must be in order: lowercase, asciifolding."""
        index = _build_index()
        analyzer = _get_analyzer(index, "name_analyzer")
        assert analyzer.token_filters == ["lowercase", "asciifolding"]


# ===========================================================================
# Test Class: Scoring Profile (same as V2)
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
        profile = next((p for p in (index.scoring_profiles or []) if p.name == "pii_boost"), None)
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
# Test Class: Complete Field List
# ===========================================================================

class TestCompleteFieldList:
    """V3: The index must contain all required V3 fields — no more, no less."""

    # V3 = V2 fields + 7 new PII metadata fields
    EXPECTED_FIELDS = {
        # V2 metadata
        "id", "md5", "file_path",
        # V2 content
        "content", "content_phonetic", "content_lowercase",
        # V3 PII booleans
        "has_ssn", "has_name", "has_dob", "has_address", "has_phone",
        # V3 PII collection + count
        "pii_types", "pii_entity_count",
    }

    def test_all_expected_fields_present(self):
        """Index must contain all 13 expected V3 fields."""
        index = _build_index()
        actual_fields = {f.name for f in index.fields}
        assert self.EXPECTED_FIELDS.issubset(actual_fields), (
            f"Missing fields: {self.EXPECTED_FIELDS - actual_fields}"
        )

    def test_no_unexpected_fields(self):
        """Index must not contain fields beyond the 13 expected V3 ones."""
        index = _build_index()
        actual_fields = {f.name for f in index.fields}
        unexpected = actual_fields - self.EXPECTED_FIELDS
        assert not unexpected, f"Unexpected fields: {unexpected}"


# ===========================================================================
# Test Class: Index Creation (create_v3_index)
# ===========================================================================

class TestCreateV3Index:
    """The create_v3_index function must call the Azure SDK to create the index."""

    def test_create_v3_index_calls_create_or_update(self, env_vars, mock_credential, mock_index_client):
        """create_v3_index must call client.create_or_update_index with the V3 index definition."""
        from scripts.create_search_index_v3 import create_v3_index

        create_v3_index()

        mock_index_client.create_or_update_index.assert_called_once()
        args, kwargs = mock_index_client.create_or_update_index.call_args
        index_arg = args[0] if args else kwargs.get("index")
        assert isinstance(index_arg, SearchIndex)
        assert index_arg.name == "breach-file-index-v3"

    def test_create_v3_index_uses_endpoint_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_v3_index must read AZURE_SEARCH_ENDPOINT from env."""
        from scripts.create_search_index_v3 import create_v3_index, SearchIndexClient

        create_v3_index()

        SearchIndexClient.assert_called_once()
        call_args = SearchIndexClient.call_args
        endpoint_used = (
            call_args[1].get("endpoint")
            or (call_args[0][0] if call_args[0] else None)
        )
        assert endpoint_used == "https://test-search.search.windows.net"

    def test_create_v3_index_uses_key_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_v3_index must use AZURE_SEARCH_KEY to create AzureKeyCredential."""
        from scripts.create_search_index_v3 import create_v3_index, AzureKeyCredential

        create_v3_index()

        AzureKeyCredential.assert_called_once_with("test-admin-key-abc123")

    def test_create_v3_index_uses_v3_index_name_from_env(self, env_vars, mock_credential, mock_index_client):
        """create_v3_index must use AZURE_SEARCH_INDEX_V3 env var as index name."""
        from scripts.create_search_index_v3 import create_v3_index

        create_v3_index()

        args, kwargs = mock_index_client.create_or_update_index.call_args
        index_arg = args[0] if args else kwargs.get("index")
        assert index_arg.name == "breach-file-index-v3"

    def test_create_v3_index_uses_default_name_when_env_not_set(self, monkeypatch, mock_credential, mock_index_client):
        """create_v3_index must default to 'breach-file-index-v3' when env var is absent."""
        monkeypatch.setenv("AZURE_SEARCH_ENDPOINT", "https://test-search.search.windows.net")
        monkeypatch.setenv("AZURE_SEARCH_KEY", "test-admin-key-abc123")
        monkeypatch.delenv("AZURE_SEARCH_INDEX_V3", raising=False)

        from scripts.create_search_index_v3 import create_v3_index

        create_v3_index()

        args, kwargs = mock_index_client.create_or_update_index.call_args
        index_arg = args[0] if args else kwargs.get("index")
        assert index_arg.name == "breach-file-index-v3"

    def test_create_v3_index_logs_success(self, env_vars, mock_credential, mock_index_client, caplog):
        """create_v3_index must log a success message after creating the index."""
        from scripts.create_search_index_v3 import create_v3_index

        mock_index_client.create_or_update_index.return_value = MagicMock(name="breach-file-index-v3")

        with caplog.at_level(logging.INFO):
            create_v3_index()

        assert any("breach-file-index-v3" in record.message for record in caplog.records)

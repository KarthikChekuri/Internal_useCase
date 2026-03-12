"""
scripts/create_search_index_v3.py — Azure AI Search index setup script (V3).

Creates the 'breach-file-index-v3' in Azure AI Search with:
- Custom phonetic_analyzer: standard tokenizer, lowercase, asciifolding,
  Double Metaphone (replace=false)
- Custom name_analyzer: standard tokenizer, lowercase, asciifolding
- Three content fields: content (standard.lucene), content_phonetic
  (phonetic_analyzer), content_lowercase (name_analyzer)
- V2 metadata fields: id (key, MD5 hash), md5, file_path
- V3 PII metadata fields: has_ssn, has_name, has_dob, has_address, has_phone
  (Boolean, filterable)
- pii_types (Collection(String), filterable)
- pii_entity_count (Int32, filterable)
- Scoring profile 'pii_boost': content=3.0, content_lowercase=2.0,
  content_phonetic=1.5

V3 additions from V2:
- has_ssn: Boolean flag — document contains SSN
- has_name: Boolean flag — document contains a person name
- has_dob: Boolean flag — document contains a date of birth
- has_address: Boolean flag — document contains an address
- has_phone: Boolean flag — document contains a phone number
- pii_types: Collection(String) — list of PII categories detected
- pii_entity_count: Int32 — total count of PII entities detected

Usage:
    python scripts/create_search_index_v3.py

Reads configuration from environment variables:
    AZURE_SEARCH_ENDPOINT   — Azure AI Search endpoint URL
    AZURE_SEARCH_KEY        — Azure AI Search admin API key
    AZURE_SEARCH_INDEX_V3   — Index name (default: breach-file-index-v3)
"""
import logging
import os

from azure.core.credentials import AzureKeyCredential
from azure.search.documents.indexes import SearchIndexClient
from azure.search.documents.indexes.models import (
    CustomAnalyzer,
    PhoneticEncoder,
    PhoneticTokenFilter,
    ScoringProfile,
    SearchableField,
    SearchField,
    SearchFieldDataType,
    SearchIndex,
    SimpleField,
    TextWeights,
)

logger = logging.getLogger(__name__)


def build_v3_index_definition(index_name: str) -> SearchIndex:
    """Build and return a SearchIndex definition for the breach-file-index-v3.

    Extends the V2 index definition with 7 additional PII metadata fields:
    - 5 Boolean filterable flags (has_ssn, has_name, has_dob, has_address, has_phone)
    - 1 Collection(String) filterable field (pii_types)
    - 1 Int32 filterable field (pii_entity_count)

    Args:
        index_name: The name to assign to the index.

    Returns:
        A fully configured SearchIndex object ready for creation.
    """
    # -----------------------------------------------------------------------
    # Custom token filters
    # -----------------------------------------------------------------------
    double_metaphone_filter = PhoneticTokenFilter(
        name="double_metaphone_filter",
        encoder=PhoneticEncoder.DOUBLE_METAPHONE,
        replace_original_tokens=False,
    )

    # -----------------------------------------------------------------------
    # Custom analyzers (same as V2)
    # -----------------------------------------------------------------------
    phonetic_analyzer = CustomAnalyzer(
        name="phonetic_analyzer",
        tokenizer_name="standard_v2",
        token_filters=["lowercase", "asciifolding", "double_metaphone_filter"],
    )

    name_analyzer = CustomAnalyzer(
        name="name_analyzer",
        tokenizer_name="standard_v2",
        token_filters=["lowercase", "asciifolding"],
    )

    # -----------------------------------------------------------------------
    # Fields — V2 metadata + content fields
    # -----------------------------------------------------------------------
    fields = [
        # Key field — MD5 hash (V2)
        SimpleField(
            name="id",
            type=SearchFieldDataType.String,
            key=True,
            filterable=True,
        ),
        # V2 metadata fields
        SimpleField(
            name="md5",
            type=SearchFieldDataType.String,
            filterable=True,
        ),
        SimpleField(
            name="file_path",
            type=SearchFieldDataType.String,
            filterable=False,
        ),
        # Content fields — same text, different analyzers (V2)
        SearchableField(
            name="content",
            type=SearchFieldDataType.String,
            analyzer_name="standard.lucene",
        ),
        SearchableField(
            name="content_phonetic",
            type=SearchFieldDataType.String,
            analyzer_name="phonetic_analyzer",
        ),
        SearchableField(
            name="content_lowercase",
            type=SearchFieldDataType.String,
            analyzer_name="name_analyzer",
        ),
        # -----------------------------------------------------------------------
        # V3 PII metadata fields — Boolean presence flags
        # -----------------------------------------------------------------------
        SimpleField(
            name="has_ssn",
            type=SearchFieldDataType.Boolean,
            filterable=True,
        ),
        SimpleField(
            name="has_name",
            type=SearchFieldDataType.Boolean,
            filterable=True,
        ),
        SimpleField(
            name="has_dob",
            type=SearchFieldDataType.Boolean,
            filterable=True,
        ),
        SimpleField(
            name="has_address",
            type=SearchFieldDataType.Boolean,
            filterable=True,
        ),
        SimpleField(
            name="has_phone",
            type=SearchFieldDataType.Boolean,
            filterable=True,
        ),
        # V3 PII collection and count fields
        SimpleField(
            name="pii_types",
            type=SearchFieldDataType.Collection(SearchFieldDataType.String),
            filterable=True,
        ),
        SimpleField(
            name="pii_entity_count",
            type=SearchFieldDataType.Int32,
            filterable=True,
        ),
    ]

    # -----------------------------------------------------------------------
    # Scoring profile (same as V2)
    # -----------------------------------------------------------------------
    pii_boost = ScoringProfile(
        name="pii_boost",
        text_weights=TextWeights(
            weights={
                "content": 3.0,
                "content_lowercase": 2.0,
                "content_phonetic": 1.5,
            }
        ),
    )

    # -----------------------------------------------------------------------
    # Assemble index
    # -----------------------------------------------------------------------
    index = SearchIndex(
        name=index_name,
        fields=fields,
        analyzers=[phonetic_analyzer, name_analyzer],
        token_filters=[double_metaphone_filter],
        scoring_profiles=[pii_boost],
        default_scoring_profile="pii_boost",
    )

    return index


def create_v3_index() -> None:
    """Create or update the breach-file-index-v3 in Azure AI Search.

    Reads configuration from environment variables:
        AZURE_SEARCH_ENDPOINT   — endpoint URL
        AZURE_SEARCH_KEY        — admin API key
        AZURE_SEARCH_INDEX_V3   — index name (default: breach-file-index-v3)
    """
    endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
    api_key = os.environ["AZURE_SEARCH_KEY"]
    index_name = os.environ.get("AZURE_SEARCH_INDEX_V3", "breach-file-index-v3")

    credential = AzureKeyCredential(api_key)
    client = SearchIndexClient(endpoint=endpoint, credential=credential)

    index_definition = build_v3_index_definition(index_name)
    result = client.create_or_update_index(index_definition)

    logger.info("Index '%s' created or updated successfully.", result.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    create_v3_index()

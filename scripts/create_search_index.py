"""
scripts/create_search_index.py — Azure AI Search index setup script (V2).

Creates the 'breach-file-index' in Azure AI Search with:
- Custom phonetic_analyzer: standard tokenizer, lowercase, asciifolding,
  Double Metaphone (replace=false)
- Custom name_analyzer: standard tokenizer, lowercase, asciifolding
- Three content fields: content (standard.lucene), content_phonetic
  (phonetic_analyzer), content_lowercase (name_analyzer)
- V2 metadata fields: id (key, MD5 hash), md5, file_path
- Scoring profile 'pii_boost': content=3.0, content_lowercase=2.0,
  content_phonetic=1.5

V2 changes from V1:
- id field is now MD5 hash (was GUID)
- Added md5 field
- Removed: file_guid, file_name, file_extension, case_name

Usage:
    python scripts/create_search_index.py

Reads configuration from environment variables:
    AZURE_SEARCH_ENDPOINT — Azure AI Search endpoint URL
    AZURE_SEARCH_KEY      — Azure AI Search admin API key
    AZURE_SEARCH_INDEX    — Index name (default: breach-file-index)
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


def build_index_definition(index_name: str) -> SearchIndex:
    """Build and return a SearchIndex definition for the breach-file-index.

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
    # Custom analyzers
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
    # Fields — V2 schema (id=MD5, md5, file_path, content fields)
    # -----------------------------------------------------------------------
    fields = [
        # Key field — MD5 hash (V2: was GUID in V1)
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
        # Content fields — same text, different analyzers
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
    ]

    # -----------------------------------------------------------------------
    # Scoring profile
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


def create_index() -> None:
    """Create or update the breach-file-index in Azure AI Search.

    Reads configuration from environment variables:
        AZURE_SEARCH_ENDPOINT — endpoint URL
        AZURE_SEARCH_KEY      — admin API key
        AZURE_SEARCH_INDEX    — index name (default: breach-file-index)
    """
    endpoint = os.environ["AZURE_SEARCH_ENDPOINT"]
    api_key = os.environ["AZURE_SEARCH_KEY"]
    index_name = os.environ.get("AZURE_SEARCH_INDEX", "breach-file-index")

    credential = AzureKeyCredential(api_key)
    client = SearchIndexClient(endpoint=endpoint, credential=credential)

    index_definition = build_index_definition(index_name)
    result = client.create_or_update_index(index_definition)

    logger.info("Index '%s' created or updated successfully.", result.name)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
    create_index()

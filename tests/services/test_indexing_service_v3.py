"""Tests for V3 Indexing Service with PII Detection (Phase V3-2.1).

V3 adds Azure AI Language PII Detection on top of V2 indexing:
- _call_pii_detection(text) -> list[dict] of entity dicts from Azure AI Language
- _map_pii_entities(entities) -> dict with has_ssn, has_name, has_dob, has_address,
  has_phone, pii_types (distinct), pii_entity_count (total)
- _build_v3_document(md5, file_path, content, pii_metadata) -> dict with all V2 fields
  plus V3 PII metadata fields
- index_all_files_v3(db, search_client, config) -> IndexResponse
- PII Detection API fallback: when API raises exception, defaults (all false/empty),
  logs warning, continues indexing

Entity type mapping:
  "SocialSecurity" in type  -> has_ssn = True
  "Person" in type          -> has_name = True
  "DateTime" or "Date" in type -> has_dob = True
  "Address" in type         -> has_address = True
  "PhoneNumber" in type     -> has_phone = True
"""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch, call

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_dlu_v3(md5="abc123", file_path="data/TEXT/abc123.txt"):
    """Return a fake DLU V3 row object (MD5 + file_path only)."""
    return SimpleNamespace(MD5=md5, file_path=file_path)


def _make_settings_v3(**overrides):
    """Return a fake Settings object for V3."""
    defaults = {
        "DATABASE_URL": "mssql+pyodbc://fake",
        "AZURE_SEARCH_ENDPOINT": "https://fake.search.windows.net",
        "AZURE_SEARCH_KEY": "fake-key",
        "AZURE_SEARCH_INDEX": "breach-file-index",
        "AZURE_SEARCH_INDEX_V3": "breach-file-index-v3",
        "AZURE_LANGUAGE_ENDPOINT": "https://fake.cognitiveservices.azure.com",
        "AZURE_LANGUAGE_KEY": "fake-lang-key",
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _make_pii_entity(category="USSocialSecurityNumber", text="123-45-6789", confidence=0.99):
    """Return a fake PII entity dict as returned by the Language API."""
    return {"category": category, "text": text, "confidence_score": confidence}


# ---------------------------------------------------------------------------
# Tests: _call_pii_detection
# ---------------------------------------------------------------------------

class TestCallPiiDetection:
    """_call_pii_detection calls Azure AI Language PII Detection API."""

    def test_returns_entity_list_with_ssn_person_datetime(self):
        """WHEN API returns SSN + Person + DateTime entities THEN returns parsed entity list."""
        from app.services.indexing_service_v3 import _call_pii_detection

        # Three entities: SSN, Person, DateTime
        fake_entity_ssn = MagicMock()
        fake_entity_ssn.category = "USSocialSecurityNumber"
        fake_entity_ssn.text = "123-45-6789"
        fake_entity_ssn.confidence_score = 0.99

        fake_entity_person = MagicMock()
        fake_entity_person.category = "Person"
        fake_entity_person.text = "John Doe"
        fake_entity_person.confidence_score = 0.95

        fake_entity_datetime = MagicMock()
        fake_entity_datetime.category = "DateTime"
        fake_entity_datetime.text = "1990-05-15"
        fake_entity_datetime.confidence_score = 0.88

        # Mock the PII result returned by recognize_pii_entities
        fake_result = MagicMock()
        fake_result.is_error = False
        fake_result.entities = [fake_entity_ssn, fake_entity_person, fake_entity_datetime]

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.return_value = [fake_result]

        entities = _call_pii_detection("John Doe SSN 123-45-6789 born 1990-05-15", client=mock_client)

        assert len(entities) == 3
        categories = [e["category"] for e in entities]
        assert "USSocialSecurityNumber" in categories
        assert "Person" in categories
        assert "DateTime" in categories

    def test_returns_empty_list_when_no_entities(self):
        """WHEN API returns no entities THEN returns empty list."""
        from app.services.indexing_service_v3 import _call_pii_detection

        fake_result = MagicMock()
        fake_result.is_error = False
        fake_result.entities = []

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.return_value = [fake_result]

        entities = _call_pii_detection("This text has no PII at all.", client=mock_client)

        assert entities == []

    def test_entity_dicts_have_category_and_text(self):
        """Each returned entity dict has 'category' and 'text' keys."""
        from app.services.indexing_service_v3 import _call_pii_detection

        fake_entity = MagicMock()
        fake_entity.category = "Address"
        fake_entity.text = "123 Main St"
        fake_entity.confidence_score = 0.90

        fake_result = MagicMock()
        fake_result.is_error = False
        fake_result.entities = [fake_entity]

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.return_value = [fake_result]

        entities = _call_pii_detection("Address: 123 Main St", client=mock_client)

        assert len(entities) == 1
        assert "category" in entities[0]
        assert "text" in entities[0]
        assert entities[0]["category"] == "Address"
        assert entities[0]["text"] == "123 Main St"

    def test_returns_empty_list_on_api_error_result(self):
        """WHEN API returns error result THEN returns empty list (graceful)."""
        from app.services.indexing_service_v3 import _call_pii_detection

        fake_result = MagicMock()
        fake_result.is_error = True

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.return_value = [fake_result]

        entities = _call_pii_detection("some text", client=mock_client)

        assert entities == []


# ---------------------------------------------------------------------------
# Tests: _map_pii_entities
# ---------------------------------------------------------------------------

class TestMapPiiEntities:
    """_map_pii_entities maps entity types to has_* booleans + pii_types + pii_entity_count."""

    def test_social_security_sets_has_ssn(self):
        """Entity type 'USSocialSecurityNumber' sets has_ssn=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "USSocialSecurityNumber", "text": "123-45-6789"}]
        result = _map_pii_entities(entities)

        assert result["has_ssn"] is True

    def test_person_sets_has_name(self):
        """Entity type 'Person' sets has_name=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "Person", "text": "John Doe"}]
        result = _map_pii_entities(entities)

        assert result["has_name"] is True

    def test_datetime_sets_has_dob(self):
        """Entity type 'DateTime' sets has_dob=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "DateTime", "text": "1990-05-15"}]
        result = _map_pii_entities(entities)

        assert result["has_dob"] is True

    def test_date_in_category_sets_has_dob(self):
        """Entity type containing 'Date' (e.g., 'Date') sets has_dob=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "Date", "text": "05/15/1990"}]
        result = _map_pii_entities(entities)

        assert result["has_dob"] is True

    def test_address_sets_has_address(self):
        """Entity type 'Address' sets has_address=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "Address", "text": "123 Main St"}]
        result = _map_pii_entities(entities)

        assert result["has_address"] is True

    def test_phone_number_sets_has_phone(self):
        """Entity type 'PhoneNumber' sets has_phone=True."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "PhoneNumber", "text": "555-867-5309"}]
        result = _map_pii_entities(entities)

        assert result["has_phone"] is True

    def test_multiple_entities_set_multiple_flags(self):
        """Multiple entity types set all corresponding has_* flags."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [
            {"category": "USSocialSecurityNumber", "text": "123-45-6789"},
            {"category": "Person", "text": "Jane Smith"},
            {"category": "DateTime", "text": "1985-03-20"},
            {"category": "Address", "text": "456 Oak Ave"},
            {"category": "PhoneNumber", "text": "800-555-0199"},
        ]
        result = _map_pii_entities(entities)

        assert result["has_ssn"] is True
        assert result["has_name"] is True
        assert result["has_dob"] is True
        assert result["has_address"] is True
        assert result["has_phone"] is True

    def test_pii_types_is_distinct_list(self):
        """pii_types is a distinct (deduplicated) list of entity category strings."""
        from app.services.indexing_service_v3 import _map_pii_entities

        # Two SSN entities — pii_types should only have one "USSocialSecurityNumber"
        entities = [
            {"category": "USSocialSecurityNumber", "text": "123-45-6789"},
            {"category": "USSocialSecurityNumber", "text": "987-65-4321"},
            {"category": "Person", "text": "Alice Brown"},
        ]
        result = _map_pii_entities(entities)

        assert result["pii_types"].count("USSocialSecurityNumber") == 1
        assert "Person" in result["pii_types"]
        assert len(result["pii_types"]) == 2  # only 2 distinct types

    def test_pii_entity_count_is_total_count(self):
        """pii_entity_count is the total number of entities (not distinct)."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [
            {"category": "USSocialSecurityNumber", "text": "123-45-6789"},
            {"category": "USSocialSecurityNumber", "text": "987-65-4321"},
            {"category": "Person", "text": "Alice Brown"},
        ]
        result = _map_pii_entities(entities)

        assert result["pii_entity_count"] == 3

    def test_no_entities_all_false_empty(self):
        """No entities -> all has_* False, pii_types=[], pii_entity_count=0."""
        from app.services.indexing_service_v3 import _map_pii_entities

        result = _map_pii_entities([])

        assert result["has_ssn"] is False
        assert result["has_name"] is False
        assert result["has_dob"] is False
        assert result["has_address"] is False
        assert result["has_phone"] is False
        assert result["pii_types"] == []
        assert result["pii_entity_count"] == 0

    def test_unknown_entity_type_does_not_crash(self):
        """Unknown entity types increment count and appear in pii_types but set no flag."""
        from app.services.indexing_service_v3 import _map_pii_entities

        entities = [{"category": "CreditCardNumber", "text": "4111-1111-1111-1111"}]
        result = _map_pii_entities(entities)

        assert result["has_ssn"] is False
        assert result["has_name"] is False
        assert result["has_dob"] is False
        assert result["has_address"] is False
        assert result["has_phone"] is False
        assert "CreditCardNumber" in result["pii_types"]
        assert result["pii_entity_count"] == 1

    def test_social_security_substring_match(self):
        """Entity type containing 'SocialSecurity' (e.g., 'USSocialSecurityNumber') sets has_ssn."""
        from app.services.indexing_service_v3 import _map_pii_entities

        # Verify substring matching works for the SocialSecurity rule
        entities = [{"category": "USSocialSecurityNumber", "text": "123-45-6789"}]
        result = _map_pii_entities(entities)
        assert result["has_ssn"] is True

    def test_result_has_all_required_keys(self):
        """_map_pii_entities result always has all required keys."""
        from app.services.indexing_service_v3 import _map_pii_entities

        result = _map_pii_entities([])
        required = {"has_ssn", "has_name", "has_dob", "has_address", "has_phone",
                    "pii_types", "pii_entity_count"}
        assert required.issubset(result.keys())


# ---------------------------------------------------------------------------
# Tests: _build_v3_document
# ---------------------------------------------------------------------------

class TestBuildV3Document:
    """_build_v3_document builds document dict with all V2 + V3 PII metadata fields."""

    def _default_pii_metadata(self, **overrides):
        defaults = {
            "has_ssn": False,
            "has_name": False,
            "has_dob": False,
            "has_address": False,
            "has_phone": False,
            "pii_types": [],
            "pii_entity_count": 0,
        }
        defaults.update(overrides)
        return defaults

    def test_document_has_all_v2_fields(self):
        """Document includes all V2 fields: id, md5, file_path, content, content_phonetic, content_lowercase."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        doc = _build_v3_document("abc123", "data/file.txt", "some text", pii_meta)

        v2_fields = {"id", "md5", "file_path", "content", "content_phonetic", "content_lowercase"}
        assert v2_fields.issubset(doc.keys())

    def test_document_has_all_v3_pii_fields(self):
        """Document includes all V3 PII metadata fields."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        doc = _build_v3_document("abc123", "data/file.txt", "some text", pii_meta)

        v3_fields = {"has_ssn", "has_name", "has_dob", "has_address", "has_phone",
                     "pii_types", "pii_entity_count"}
        assert v3_fields.issubset(doc.keys())

    def test_document_id_equals_md5(self):
        """Document id field equals the md5 argument."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        doc = _build_v3_document("mymd5hash", "data/file.txt", "text", pii_meta)

        assert doc["id"] == "mymd5hash"
        assert doc["md5"] == "mymd5hash"

    def test_document_file_path(self):
        """Document file_path matches the file_path argument."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        fp = "data/TEXT/c85/c8578af0e239aaeb7e4030b346430ac3.txt"
        doc = _build_v3_document("c8578af0e239aaeb7e4030b346430ac3", fp, "hello", pii_meta)

        assert doc["file_path"] == fp

    def test_document_content_fields_all_same_text(self):
        """content, content_phonetic, content_lowercase all equal the content argument."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        text = "John Doe SSN 123-45-6789"
        doc = _build_v3_document("md5x", "data/a.txt", text, pii_meta)

        assert doc["content"] == text
        assert doc["content_phonetic"] == text
        assert doc["content_lowercase"] == text

    def test_document_pii_metadata_with_ssn_and_name(self):
        """PII metadata fields are correctly propagated into the document."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata(
            has_ssn=True,
            has_name=True,
            pii_types=["USSocialSecurityNumber", "Person"],
            pii_entity_count=2,
        )
        doc = _build_v3_document("md5y", "data/b.csv", "text with pii", pii_meta)

        assert doc["has_ssn"] is True
        assert doc["has_name"] is True
        assert doc["has_dob"] is False
        assert doc["has_address"] is False
        assert doc["has_phone"] is False
        assert doc["pii_types"] == ["USSocialSecurityNumber", "Person"]
        assert doc["pii_entity_count"] == 2

    def test_document_default_pii_metadata_all_false(self):
        """Document with no-PII metadata has all has_* False, pii_types=[], count=0."""
        from app.services.indexing_service_v3 import _build_v3_document

        pii_meta = self._default_pii_metadata()
        doc = _build_v3_document("md5z", "data/c.txt", "no pii here", pii_meta)

        assert doc["has_ssn"] is False
        assert doc["has_name"] is False
        assert doc["has_dob"] is False
        assert doc["has_address"] is False
        assert doc["has_phone"] is False
        assert doc["pii_types"] == []
        assert doc["pii_entity_count"] == 0


# ---------------------------------------------------------------------------
# Tests: PII Detection API fallback
# ---------------------------------------------------------------------------

class TestPiiDetectionFallback:
    """When PII Detection API raises exception, indexing continues with defaults."""

    def test_call_pii_detection_raises_exception_returns_empty_list(self):
        """WHEN API call raises exception THEN _call_pii_detection returns empty list."""
        from app.services.indexing_service_v3 import _call_pii_detection

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.side_effect = Exception("Network error")

        entities = _call_pii_detection("some text", client=mock_client)

        assert entities == []

    def test_pii_detection_exception_logs_warning(self, caplog):
        """WHEN API call raises exception THEN a warning is logged."""
        import logging
        from app.services.indexing_service_v3 import _call_pii_detection

        mock_client = MagicMock()
        mock_client.recognize_pii_entities.side_effect = Exception("Timeout")

        with caplog.at_level(logging.WARNING):
            _call_pii_detection("some text", client=mock_client)

        # At least one warning should be logged
        warnings = [r for r in caplog.records if r.levelno >= logging.WARNING]
        assert len(warnings) >= 1

    def test_index_all_files_v3_continues_when_pii_api_fails(self):
        """WHEN PII Detection API fails THEN document is still indexed with default metadata."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5a", file_path="data/a.txt")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="some content"), \
             patch("app.services.indexing_service_v3._call_pii_detection",
                   return_value=[]) as mock_pii, \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        # Indexing should succeed despite API fallback
        assert result.files_processed == 1
        assert result.files_succeeded == 1
        assert result.files_failed == 0

    def test_index_all_files_v3_document_has_default_pii_on_api_failure(self):
        """WHEN PII Detection API fails THEN uploaded document has all has_*=False."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5a", file_path="data/a.txt")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()
        captured_docs = []

        def capture_upload(search_client, docs):
            captured_docs.extend(docs)
            return []

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="some content"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3",
                   side_effect=capture_upload):

            index_all_files_v3(db, search_client, config)

        assert len(captured_docs) == 1
        doc = captured_docs[0]
        assert doc["has_ssn"] is False
        assert doc["has_name"] is False
        assert doc["has_dob"] is False
        assert doc["has_address"] is False
        assert doc["has_phone"] is False
        assert doc["pii_types"] == []
        assert doc["pii_entity_count"] == 0


# ---------------------------------------------------------------------------
# Tests: index_all_files_v3 orchestration
# ---------------------------------------------------------------------------

class TestIndexAllFilesV3:
    """index_all_files_v3 orchestrates query -> filter -> extract -> PII detect -> upload."""

    def test_correct_number_of_documents_uploaded(self):
        """WHEN 3 eligible files THEN 3 documents uploaded."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [
            _make_dlu_v3(md5=f"md5_{i}", file_path=f"data/file_{i}.txt")
            for i in range(3)
        ]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()
        captured_docs = []

        def capture_upload(sc, docs):
            captured_docs.extend(docs)
            return []

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="file content"), \
             patch("app.services.indexing_service_v3._call_pii_detection",
                   return_value=[{"category": "Person", "text": "Test User"}]), \
             patch("app.services.indexing_service_v3._upload_documents_v3",
                   side_effect=capture_upload):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 3
        assert result.files_succeeded == 3
        assert result.files_failed == 0
        assert len(captured_docs) == 3

    def test_uploaded_documents_have_pii_metadata(self):
        """Uploaded documents contain PII metadata fields from detection."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5ssn", file_path="data/ssn_file.txt")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()
        captured_docs = []

        def capture_upload(sc, docs):
            captured_docs.extend(docs)
            return []

        pii_entities = [
            {"category": "USSocialSecurityNumber", "text": "123-45-6789"},
            {"category": "Person", "text": "Jane Doe"},
        ]

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="Jane Doe SSN 123-45-6789"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=pii_entities), \
             patch("app.services.indexing_service_v3._upload_documents_v3",
                   side_effect=capture_upload):

            index_all_files_v3(db, search_client, config)

        assert len(captured_docs) == 1
        doc = captured_docs[0]
        assert doc["has_ssn"] is True
        assert doc["has_name"] is True
        assert doc["has_dob"] is False
        assert "USSocialSecurityNumber" in doc["pii_types"]
        assert "Person" in doc["pii_types"]
        assert doc["pii_entity_count"] == 2

    def test_empty_dlu_returns_zero_counts(self):
        """Empty DLU -> 0 processed, 0 succeeded, 0 failed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 0
        assert result.files_succeeded == 0
        assert result.files_failed == 0

    def test_extraction_failure_counts_as_failed(self):
        """WHEN text extraction fails THEN file counted as failed, not uploaded."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="bad_md5", file_path="data/corrupt.xlsx")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value=None), \
             patch("app.services.indexing_service_v3._call_pii_detection") as mock_pii, \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 1
        assert result.files_succeeded == 0
        assert result.files_failed == 1
        mock_pii.assert_not_called()

    def test_returns_index_response_object(self):
        """index_all_files_v3 returns an IndexResponse (or compatible object)."""
        from app.services.indexing_service_v3 import index_all_files_v3

        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        # Must have these fields
        assert hasattr(result, "files_processed")
        assert hasattr(result, "files_succeeded")
        assert hasattr(result, "files_failed")
        assert hasattr(result, "errors")


# ---------------------------------------------------------------------------
# Tests: File extension filtering (same as V2)
# ---------------------------------------------------------------------------

class TestFileExtensionFilteringV3:
    """V3 only indexes .txt, .csv, .xls, .xlsx files (same as V2)."""

    def test_txt_file_processed(self):
        """WHEN file_path ends in .txt THEN file is processed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5txt", file_path="data/file.txt")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="text content"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 1
        assert result.files_succeeded == 1

    def test_csv_file_processed(self):
        """WHEN file_path ends in .csv THEN file is processed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5csv", file_path="data/data.csv")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="col1,col2"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 1

    def test_xlsx_file_processed(self):
        """WHEN file_path ends in .xlsx THEN file is processed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5xlsx", file_path="data/report.xlsx")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="row1 col1"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 1

    def test_xls_file_processed(self):
        """WHEN file_path ends in .xls THEN file is processed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5xls", file_path="data/legacy.xls")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="sheet data"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 1

    def test_pdf_file_skipped(self):
        """WHEN file_path ends in .pdf THEN file is skipped (not processed)."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5pdf", file_path="data/invoice.pdf")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text") as mock_extract, \
             patch("app.services.indexing_service_v3._call_pii_detection") as mock_pii, \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 0
        assert result.files_succeeded == 0
        mock_extract.assert_not_called()
        mock_pii.assert_not_called()

    def test_mp4_file_skipped(self):
        """WHEN file_path ends in .mp4 THEN file is skipped."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [_make_dlu_v3(md5="md5mp4", file_path="data/video.mp4")]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text") as mock_extract, \
             patch("app.services.indexing_service_v3._call_pii_detection") as mock_pii, \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        assert result.files_processed == 0
        mock_extract.assert_not_called()
        mock_pii.assert_not_called()

    def test_mixed_extensions_only_supported_processed(self):
        """WHEN DLU has mixed extensions THEN only .txt/.csv/.xls/.xlsx are processed."""
        from app.services.indexing_service_v3 import index_all_files_v3

        records = [
            _make_dlu_v3(md5="md5txt", file_path="data/a.txt"),
            _make_dlu_v3(md5="md5pdf", file_path="data/b.pdf"),
            _make_dlu_v3(md5="md5csv", file_path="data/c.csv"),
            _make_dlu_v3(md5="md5mp4", file_path="data/d.mp4"),
        ]
        config = _make_settings_v3()
        db = MagicMock()
        search_client = MagicMock()

        with patch("app.services.indexing_service_v3._query_all_dlu_records_v3", return_value=records), \
             patch("app.services.indexing_service_v3.extract_text", return_value="content"), \
             patch("app.services.indexing_service_v3._call_pii_detection", return_value=[]), \
             patch("app.services.indexing_service_v3._upload_documents_v3", return_value=[]):

            result = index_all_files_v3(db, search_client, config)

        # Only .txt and .csv are processed (2 out of 4)
        assert result.files_processed == 2
        assert result.files_succeeded == 2

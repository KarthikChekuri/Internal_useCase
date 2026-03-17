"""Tests for app.models.pii — FieldMatchResult and CustomerSummary schemas."""

import pytest
from pydantic import ValidationError


class TestFieldMatchResult:
    """Tests for the FieldMatchResult schema."""

    def test_field_match_result_found_with_snippet(self):
        """A found field match includes method, confidence, and snippet."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(
            found=True,
            method="fuzzy",
            confidence=0.85,
            snippet="...Karthik Chekuri found on line 42...",
        )
        assert result.found is True
        assert result.method == "fuzzy"
        assert result.confidence == 0.85
        assert result.snippet == "...Karthik Chekuri found on line 42..."

    def test_field_match_result_not_found(self):
        """A not-found field match has found=False, zero confidence, no snippet."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(
            found=False,
            method="exact",
            confidence=0.0,
            snippet=None,
        )
        assert result.found is False
        assert result.method == "exact"
        assert result.confidence == 0.0
        assert result.snippet is None

    def test_field_match_result_snippet_is_optional(self):
        """snippet defaults to None when not provided."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(
            found=True,
            method="regex",
            confidence=0.95,
        )
        assert result.snippet is None

    def test_field_match_result_serialization(self):
        """FieldMatchResult serializes to dict with correct keys."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(
            found=True,
            method="fuzzy",
            confidence=0.75,
            snippet="...SSN 343-43-4343...",
        )
        data = result.model_dump()
        assert data == {
            "found": True,
            "method": "fuzzy",
            "confidence": 0.75,
            "snippet": "...SSN 343-43-4343...",
        }

    def test_field_match_result_confidence_boundary_zero(self):
        """Confidence of 0.0 is valid."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(found=False, method="exact", confidence=0.0)
        assert result.confidence == 0.0

    def test_field_match_result_confidence_boundary_one(self):
        """Confidence of 1.0 is valid."""
        from app.models.pii import FieldMatchResult

        result = FieldMatchResult(found=True, method="exact", confidence=1.0)
        assert result.confidence == 1.0


class TestCustomerSummary:
    """Tests for the CustomerSummary schema with SSN masking."""

    def test_customer_summary_ssn_masking_dashed(self):
        """SSN '343-43-4343' is masked to 'XXX-XX-4343'."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="Karthik Chekuri", ssn="343-43-4343")
        assert customer.ssn_masked == "XXX-XX-4343"

    def test_customer_summary_ssn_masking_undashed(self):
        """SSN '343434343' (no dashes) is masked to 'XXX-XX-4343'."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="Karthik Chekuri", ssn="343434343")
        assert customer.ssn_masked == "XXX-XX-4343"

    def test_customer_summary_fullname_preserved(self):
        """Fullname is stored as-is."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="John Doe", ssn="123-45-6789")
        assert customer.fullname == "John Doe"

    def test_customer_summary_serialization_excludes_raw_ssn(self):
        """When serialized, 'ssn' (raw) should NOT appear; only 'ssn_masked'."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="Jane Smith", ssn="111-22-3333")
        data = customer.model_dump()
        assert "ssn" not in data, "Raw SSN should be excluded from serialized output"
        assert data["ssn_masked"] == "XXX-XX-3333"
        assert data["fullname"] == "Jane Smith"

    def test_customer_summary_serialization_shape(self):
        """Serialized CustomerSummary has exactly fullname and ssn_masked."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="Mary O'Brien", ssn="999-88-7777")
        data = customer.model_dump()
        assert set(data.keys()) == {"fullname", "ssn_masked"}

    def test_customer_summary_ssn_masking_various_last4(self):
        """Masking always shows the last 4 digits correctly."""
        from app.models.pii import CustomerSummary

        customer = CustomerSummary(fullname="Test User", ssn="000-00-0001")
        assert customer.ssn_masked == "XXX-XX-0001"

        customer2 = CustomerSummary(fullname="Test User", ssn="999999999")
        assert customer2.ssn_masked == "XXX-XX-9999"

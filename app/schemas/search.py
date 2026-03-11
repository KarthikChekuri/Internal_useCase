"""Pydantic schemas for search request and response models."""

import re
import uuid
from typing import Optional

from pydantic import BaseModel, field_validator

from app.schemas.pii import CustomerSummary, FieldMatchResult


class SearchRequest(BaseModel):
    """Request model for the POST /search endpoint.

    Attributes:
        ssn: Social Security Number in XXX-XX-XXXX or XXXXXXXXX format.
        fullname: Optional customer name for pre-search validation.
    """

    ssn: str
    fullname: Optional[str] = None

    @field_validator("ssn")
    @classmethod
    def validate_ssn_format(cls, v: str) -> str:
        """Validate SSN matches XXX-XX-XXXX (dashed) or XXXXXXXXX (undashed)."""
        dashed = re.fullmatch(r"\d{3}-\d{2}-\d{4}", v)
        undashed = re.fullmatch(r"\d{9}", v)
        if not dashed and not undashed:
            raise ValueError(
                "SSN must be in XXX-XX-XXXX or XXXXXXXXX format"
            )
        return v


class FileResult(BaseModel):
    """A single file's PII leak detection result.

    Attributes:
        file_name: Name of the file that was analyzed.
        file_guid: Unique identifier of the file in the data lake.
        leaked_fields: List of PII field names detected in the file.
        overall_confidence: Aggregated confidence score for all leaked fields.
        azure_search_score: Raw relevance score from Azure AI Search.
        needs_review: Whether the result requires human review.
        match_details: Per-field match results keyed by field name.
    """

    file_name: str
    file_guid: str
    leaked_fields: list[str]
    overall_confidence: float
    azure_search_score: float
    needs_review: bool
    match_details: dict[str, FieldMatchResult]


class SearchResponse(BaseModel):
    """Response model for the POST /search endpoint.

    Attributes:
        search_run_id: UUID identifying this search execution.
        customer: Summary of the customer with masked SSN.
        results: List of file results with leak detection details.
    """

    search_run_id: uuid.UUID
    customer: CustomerSummary
    results: list[FileResult]

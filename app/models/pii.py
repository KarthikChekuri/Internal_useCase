"""Pydantic schemas for PII field match results and customer summary."""

import re
from typing import Optional

from pydantic import BaseModel, model_serializer


class FieldMatchResult(BaseModel):
    """Result of matching a single PII field against a file's content.

    Attributes:
        found: Whether the field was detected in the file.
        method: The matching method used (e.g., 'exact', 'fuzzy', 'regex').
        confidence: Confidence score between 0.0 and 1.0.
        snippet: Optional contextual snippet showing where the match was found.
    """

    found: bool
    method: str
    confidence: float
    snippet: Optional[str] = None


class CustomerSummary(BaseModel):
    """Summary of a customer for search responses. Masks the SSN to show only last 4 digits.

    Accepts raw SSN (dashed or undashed) on construction, but serializes with only
    `fullname` and `ssn_masked`. The raw SSN is excluded from serialized output.

    Attributes:
        fullname: The customer's full name.
        ssn: The raw SSN (excluded from serialization).
        ssn_masked: Computed property that masks the SSN as 'XXX-XX-{last4}'.
    """

    fullname: str
    ssn: str

    @property
    def ssn_masked(self) -> str:
        """Mask SSN to show only last 4 digits: XXX-XX-{last4}."""
        digits = re.sub(r"[^0-9]", "", self.ssn)
        last4 = digits[-4:]
        return f"XXX-XX-{last4}"

    @model_serializer
    def serialize_model(self) -> dict:
        """Serialize excluding the raw SSN, including computed ssn_masked."""
        return {
            "fullname": self.fullname,
            "ssn_masked": self.ssn_masked,
        }

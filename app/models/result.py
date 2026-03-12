"""
ORM model for [Search].[results] — V2 replacement for search_result.py.

One row per (batch_id, customer_id, md5) triplet where at least one PII
field was detected in the candidate file.

leaked_fields  — JSON array of field names confirmed present (NVARCHAR MAX)
match_details  — JSON object with per-field detection detail (NVARCHAR MAX)
"""

from __future__ import annotations

from typing import Optional
import datetime

from sqlalchemy import (
    Boolean,
    DateTime,
    Float,
    ForeignKey,
    Identity,
    Integer,
    String,
    Unicode,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


# Mapping from PII field name to ORM column name for leaked_* columns
PII_FIELD_TO_LEAKED_COLUMN = {
    "Fullname": "leaked_fullname",
    "FirstName": "leaked_firstname",
    "LastName": "leaked_lastname",
    "DOB": "leaked_dob",
    "SSN": "leaked_ssn",
    "DriversLicense": "leaked_driverslicense",
    "Address1": "leaked_address1",
    "Address2": "leaked_address2",
    "Address3": "leaked_address3",
    "ZipCode": "leaked_zipcode",
    "City": "leaked_city",
    "State": "leaked_state",
    "Country": "leaked_country",
}


class Result(Base):
    """[Search].[results] — one row per (customer, file) detection result."""

    __tablename__ = "results"
    __table_args__ = {"schema": "Search"}

    id: Mapped[int] = mapped_column(Integer, Identity(start=1), primary_key=True)

    # --- Foreign keys ---
    batch_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("Batch.batch_runs.batch_id"),
        nullable=False,
    )
    customer_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("PII.master_data.customer_id"),
        nullable=False,
    )
    md5: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("DLU.datalakeuniverse.MD5"),
        nullable=False,
    )

    # --- Detection metadata ---
    strategy_name: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_fields: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    match_details: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)

    # --- Per-field leaked text (snippet found in file, NULL if not found) ---
    leaked_fullname: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_firstname: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_lastname: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_dob: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_ssn: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_driverslicense: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_address1: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_address2: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_address3: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_zipcode: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_city: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_state: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    leaked_country: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)

    # --- Scoring ---
    overall_confidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    azure_search_score: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # --- Review flag ---
    needs_review: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # --- Timestamp ---
    searched_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime,
        server_default=text("GETDATE()"),
        nullable=True,
    )

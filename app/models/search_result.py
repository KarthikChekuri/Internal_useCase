"""
ORM model for [Search].[search_results].

Each row records the outcome of searching a single customer's PII
against a single breach file, including per-field leak flags,
confidence scores, and audit metadata.
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


class SearchResult(Base):
    """[Search].[search_results] — one row per (customer, file) pair."""

    __tablename__ = "search_results"
    __table_args__ = {"schema": "Search"}

    ID: Mapped[int] = mapped_column(
        Integer, Identity(start=1), primary_key=True
    )

    # Run identification
    SearchRunID: Mapped[Optional[str]] = mapped_column(
        String(36), nullable=True
    )

    # Foreign keys
    CustomerID: Mapped[Optional[str]] = mapped_column(
        String(10),
        nullable=True,
    )
    FileGUID: Mapped[Optional[str]] = mapped_column(
        String(250),
        ForeignKey("DLU.datalakeuniverse.GUID"),
        nullable=True,
    )

    # 13 per-field leak BIT columns
    LeakedFullname: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedFirstName: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedLastName: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedDOB: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedSSN: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedDriversLicense: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedAddress1: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedAddress2: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedAddress3: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedZipCode: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedCity: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedState: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    LeakedCountry: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)

    # JSON columns (stored as NVARCHAR(MAX))
    LeakedFieldsList: Mapped[Optional[str]] = mapped_column(
        Unicode(None), nullable=True
    )
    MatchDetails: Mapped[Optional[str]] = mapped_column(
        Unicode(None), nullable=True
    )

    # Scoring
    OverallConfidence: Mapped[Optional[float]] = mapped_column(Float, nullable=True)
    AzureSearchScore: Mapped[Optional[float]] = mapped_column(Float, nullable=True)

    # Audit / review
    NeedsReview: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    SearchedAt: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime,
        server_default=text("GETDATE()"),
        nullable=True,
    )

"""
ORM model for [PII].[master_pii].

Stores the known PII for each customer that will be searched against
breach files. This is the source-of-truth for customer identity.
"""

from __future__ import annotations

from typing import Optional
import datetime

from sqlalchemy import Date, Identity, Integer, String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class MasterPII(Base):
    """[PII].[master_pii] — one row per customer."""

    __tablename__ = "master_pii"
    __table_args__ = {"schema": "PII"}

    ID: Mapped[int] = mapped_column(
        Integer, Identity(start=1), primary_key=True
    )
    Fullname: Mapped[Optional[str]] = mapped_column(Unicode(250), nullable=True)
    FirstName: Mapped[Optional[str]] = mapped_column(Unicode(100), nullable=True)
    LastName: Mapped[Optional[str]] = mapped_column(Unicode(100), nullable=True)
    DOB: Mapped[Optional[datetime.date]] = mapped_column(Date, nullable=True)
    SSN: Mapped[Optional[str]] = mapped_column(String(11), nullable=True)
    DriversLicense: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)
    Address1: Mapped[Optional[str]] = mapped_column(Unicode(250), nullable=True)
    Address2: Mapped[Optional[str]] = mapped_column(Unicode(250), nullable=True)
    Address3: Mapped[Optional[str]] = mapped_column(Unicode(250), nullable=True)
    ZipCode: Mapped[Optional[str]] = mapped_column(String(10), nullable=True)
    City: Mapped[Optional[str]] = mapped_column(Unicode(100), nullable=True)
    State: Mapped[Optional[str]] = mapped_column(String(2), nullable=True)
    Country: Mapped[Optional[str]] = mapped_column(String(50), nullable=True)

"""
ORM model for [PII].[master_data] — V2 replacement for master_pii.

customer_id is an INT primary key that is NOT auto-generated (Identity).
It comes from the external master customer list and is managed upstream.

13 PII fields: Fullname, FirstName, LastName, DOB, SSN, DriversLicense,
               Address1, Address2, Address3, ZipCode, City, State, Country
"""

from __future__ import annotations

from typing import Optional
import datetime

from sqlalchemy import Date, Integer, String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class MasterData(Base):
    """[PII].[master_data] — one row per customer."""

    __tablename__ = "master_data"
    __table_args__ = {"schema": "PII"}

    customer_id: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=False
    )

    # --- 13 PII fields (all nullable) ---
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

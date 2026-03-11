"""
ORM model for the existing [DLU].[datalakeuniverse] table.

This table is read-only — it is managed by the upstream Data Lake Universe
pipeline and is never modified by this application.
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import Boolean, String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class DLU(Base):
    """Read-only mapping of [DLU].[datalakeuniverse]."""

    __tablename__ = "datalakeuniverse"
    __table_args__ = {"schema": "DLU"}

    GUID: Mapped[str] = mapped_column(String(250), primary_key=True)
    TEXTPATH: Mapped[Optional[str]] = mapped_column(Unicode(500), nullable=True)
    fileName: Mapped[Optional[str]] = mapped_column(Unicode(500), nullable=True)
    fileExtension: Mapped[Optional[str]] = mapped_column(Unicode(50), nullable=True)
    caseName: Mapped[Optional[str]] = mapped_column(Unicode(250), nullable=True)
    isExclusion: Mapped[Optional[bool]] = mapped_column(Boolean, nullable=True)
    MD5: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

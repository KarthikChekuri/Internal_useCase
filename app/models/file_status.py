"""
ORM model for [Index].[file_status].

Tracks whether each breach file has been indexed into Azure AI Search.
md5 is both the PK and a FK to [DLU].[datalakeuniverse].MD5.
"""

from __future__ import annotations

from typing import Optional
import datetime

from sqlalchemy import DateTime, ForeignKey, String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class FileStatus(Base):
    """[Index].[file_status] — one row per breach file."""

    __tablename__ = "file_status"
    __table_args__ = {"schema": "Index"}

    md5: Mapped[str] = mapped_column(
        String(32),
        ForeignKey("DLU.datalakeuniverse.MD5"),
        primary_key=True,
    )
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    indexed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True
    )
    error_message: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)

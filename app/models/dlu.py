"""
ORM model for [DLU].[datalakeuniverse] — V2 rewrite.

V2 simplification: MD5 is the primary key (VARCHAR 32).
file_path holds the path to the file on disk.

V1 columns removed: GUID, TEXTPATH, fileName, fileExtension, caseName, isExclusion
"""

from __future__ import annotations

from typing import Optional

from sqlalchemy import String, Unicode
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class DLU(Base):
    """[DLU].[datalakeuniverse] — one row per breach file."""

    __tablename__ = "datalakeuniverse"
    __table_args__ = {"schema": "DLU"}

    MD5: Mapped[str] = mapped_column(String(32), primary_key=True)
    file_path: Mapped[Optional[str]] = mapped_column(Unicode(500), nullable=True)

"""
ORM models for batch processing tables.

BatchRun   → [Batch].[batch_runs]    — top-level unit of work
CustomerStatus → [Batch].[customer_status] — per-customer tracking within a batch
"""

from __future__ import annotations

from typing import Optional
import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    String,
    Unicode,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.models.database import Base


class BatchRun(Base):
    """
    [Batch].[batch_runs] — one row per batch execution.

    batch_id is a UNIQUEIDENTIFIER (UUID) stored as a string.
    strategy_set stores the JSON-serialised list of strategies used.
    """

    __tablename__ = "batch_runs"
    __table_args__ = {"schema": "Batch"}

    batch_id: Mapped[str] = mapped_column(String(36), primary_key=True)
    strategy_set: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    started_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True
    )
    completed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True
    )
    total_customers: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    total_files: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)


class CustomerStatus(Base):
    """
    [Batch].[customer_status] — one row per (batch_id, customer_id) pair.

    Tracks where each customer is in the processing pipeline.
    strategies_matched stores the JSON list of strategy names that returned results.
    """

    __tablename__ = "customer_status"
    __table_args__ = {"schema": "Batch"}

    id: Mapped[int] = mapped_column(Integer, Identity(start=1), primary_key=True)
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
    status: Mapped[str] = mapped_column(String(20), nullable=False)
    candidates_found: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    leaks_confirmed: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    strategies_matched: Mapped[Optional[str]] = mapped_column(
        Unicode(None), nullable=True
    )
    error_message: Mapped[Optional[str]] = mapped_column(Unicode(None), nullable=True)
    processed_at: Mapped[Optional[datetime.datetime]] = mapped_column(
        DateTime, nullable=True
    )

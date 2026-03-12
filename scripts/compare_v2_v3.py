"""Compare V2 and V3 batch results side-by-side.

Usage:
    python scripts/compare_v2_v3.py --v2-batch <V2_BATCH_ID> --v3-batch <V3_BATCH_ID>

Queries [Search].[results] for both batches, then for each customer prints:
  - Files found by both V2 and V3
  - Files found by V2 only
  - Files found by V3 only
  - Per-file confidence score differences
  - Per-file leaked-field differences

This script avoids importing sqlalchemy at module level to prevent hangs in
environments where the DB is unreachable. All DB access is deferred to main().
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from typing import Any

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Public: get_batch_results
# ---------------------------------------------------------------------------


def get_batch_results(db: Any, batch_id: str) -> list[Any]:
    """Query [Search].[results] for all rows belonging to a batch.

    Args:
        db: SQLAlchemy Session.
        batch_id: UUID string of the target batch run.

    Returns:
        List of Result ORM rows (or SimpleNamespace objects in tests).
    """
    # Import deferred to avoid hang at module level
    from app.models.result import Result

    rows = (
        db.query(Result)
        .filter(Result.batch_id == batch_id)
        .all()
    )
    logger.info("Batch %s: found %d result rows.", batch_id, len(rows))
    return rows


# ---------------------------------------------------------------------------
# Public: compare_results
# ---------------------------------------------------------------------------


def compare_results(
    v2_results: list[Any],
    v3_results: list[Any],
) -> dict[int, dict]:
    """Compare V2 and V3 result rows per customer.

    For each customer_id, builds a dict describing:
    - "both":       set of md5 strings found in both V2 and V3
    - "v2_only":    set of md5 strings found only in V2
    - "v3_only":    set of md5 strings found only in V3
    - "doc_details": dict of md5 -> {v2_confidence, v3_confidence, v2_fields, v3_fields}

    Args:
        v2_results: List of Result rows for the V2 batch.
        v3_results: List of Result rows for the V3 batch.

    Returns:
        Dict keyed by customer_id.
    """
    # Build lookup: customer_id -> {md5 -> row} for each version
    def _index(rows: list[Any]) -> dict[int, dict[str, Any]]:
        idx: dict[int, dict[str, Any]] = {}
        for row in rows:
            cid = row.customer_id
            if cid not in idx:
                idx[cid] = {}
            idx[cid][row.md5] = row
        return idx

    v2_idx = _index(v2_results)
    v3_idx = _index(v3_results)

    all_customer_ids = set(v2_idx.keys()) | set(v3_idx.keys())
    comparison: dict[int, dict] = {}

    for cid in sorted(all_customer_ids):
        v2_docs = v2_idx.get(cid, {})
        v3_docs = v3_idx.get(cid, {})

        v2_md5s = set(v2_docs.keys())
        v3_md5s = set(v3_docs.keys())

        both_md5s = v2_md5s & v3_md5s
        v2_only_md5s = v2_md5s - v3_md5s
        v3_only_md5s = v3_md5s - v2_md5s

        # Build per-document detail dict for all docs encountered
        doc_details: dict[str, dict] = {}
        all_md5s = v2_md5s | v3_md5s

        for md5 in all_md5s:
            v2_row = v2_docs.get(md5)
            v3_row = v3_docs.get(md5)

            v2_conf = v2_row.overall_confidence if v2_row else None
            v3_conf = v3_row.overall_confidence if v3_row else None

            v2_fields: list[str] = []
            v3_fields: list[str] = []

            if v2_row and v2_row.leaked_fields:
                try:
                    v2_fields = json.loads(v2_row.leaked_fields)
                except (json.JSONDecodeError, TypeError):
                    v2_fields = []

            if v3_row and v3_row.leaked_fields:
                try:
                    v3_fields = json.loads(v3_row.leaked_fields)
                except (json.JSONDecodeError, TypeError):
                    v3_fields = []

            doc_details[md5] = {
                "v2_confidence": v2_conf,
                "v3_confidence": v3_conf,
                "v2_fields": v2_fields,
                "v3_fields": v3_fields,
            }

        comparison[cid] = {
            "both": both_md5s,
            "v2_only": v2_only_md5s,
            "v3_only": v3_only_md5s,
            "doc_details": doc_details,
        }

    return comparison


# ---------------------------------------------------------------------------
# Public: format_comparison
# ---------------------------------------------------------------------------


def format_comparison(comparison: dict[int, dict]) -> str:
    """Format the per-customer comparison dict as a console-friendly string.

    Output format per customer:
        === Customer <id> ===
          Files found by both: doc_A, doc_B
          V2 only: doc_C
          V3 only: doc_D
          [blank line for each shared doc with confidence/field differences]
          V2 confidence for doc_A: 0.92  |  V3 confidence: 0.68
          V2 fields: SSN, Fullname, DOB  |  V3 fields: SSN, Fullname

    Args:
        comparison: Dict as returned by compare_results.

    Returns:
        Formatted string for printing to console.
    """
    if not comparison:
        return "(no comparison data — both batches have zero results)"

    lines: list[str] = []

    for cid in sorted(comparison.keys()):
        cust = comparison[cid]
        both = sorted(cust.get("both", set()))
        v2_only = sorted(cust.get("v2_only", set()))
        v3_only = sorted(cust.get("v3_only", set()))
        doc_details: dict[str, dict] = cust.get("doc_details", {})

        lines.append(f"=== Customer {cid} ===")

        # Files found by both
        both_str = ", ".join(both) if both else "(none)"
        lines.append(f"  Files found by both: {both_str}")

        # V2 only
        if v2_only:
            for md5 in v2_only:
                dd = doc_details.get(md5, {})
                v2_fields = dd.get("v2_fields", [])
                fields_str = ", ".join(v2_fields) if v2_fields else "(no fields recorded)"
                lines.append(f"  V2 only: {md5} ({fields_str})")
        else:
            lines.append("  V2 only: (none)")

        # V3 only
        if v3_only:
            for md5 in v3_only:
                dd = doc_details.get(md5, {})
                v3_fields = dd.get("v3_fields", [])
                fields_str = ", ".join(v3_fields) if v3_fields else "(no fields recorded)"
                lines.append(f"  V3 only: {md5} ({fields_str})")
        else:
            lines.append("  V3 only: (none)")

        # Per-document confidence and field comparison for docs in both
        if both:
            lines.append("")
        for md5 in both:
            dd = doc_details.get(md5, {})
            v2_conf = dd.get("v2_confidence")
            v3_conf = dd.get("v3_confidence")
            v2_fields = dd.get("v2_fields", [])
            v3_fields = dd.get("v3_fields", [])

            v2_conf_str = f"{v2_conf:.4f}" if v2_conf is not None else "N/A"
            v3_conf_str = f"{v3_conf:.4f}" if v3_conf is not None else "N/A"
            v2_fields_str = ", ".join(sorted(v2_fields)) if v2_fields else "(none)"
            v3_fields_str = ", ".join(sorted(v3_fields)) if v3_fields else "(none)"

            lines.append(
                f"  V2 confidence for {md5}: {v2_conf_str}  |  V3 confidence: {v3_conf_str}"
            )
            lines.append(
                f"  V2 fields: {v2_fields_str}  |  V3 fields: {v3_fields_str}"
            )

        lines.append("")  # blank line between customers

    return "\n".join(lines)


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------


def main():
    """Parse arguments, query DB, compare results, and print formatted output."""
    parser = argparse.ArgumentParser(
        description="Compare V2 and V3 batch results from [Search].[results]."
    )
    parser.add_argument("--v2-batch", required=True, help="UUID of the V2 batch run to compare.")
    parser.add_argument("--v3-batch", required=True, help="UUID of the V3 batch run to compare.")
    args = parser.parse_args()

    # Deferred imports: only needed when actually running (not in unit tests)
    import sqlalchemy
    from app.config import get_settings
    from app.models.database import get_session_factory

    settings = get_settings()
    engine = sqlalchemy.create_engine(
        settings.DATABASE_URL,
        echo=False,
        use_setinputsizes=False,
    )
    session_factory = get_session_factory(engine)
    db = session_factory()

    try:
        v2_results = get_batch_results(db, args.v2_batch)
        v3_results = get_batch_results(db, args.v3_batch)

        if not v2_results and not v3_results:
            print(
                f"No results found for either batch.\n"
                f"  V2 batch: {args.v2_batch}\n"
                f"  V3 batch: {args.v3_batch}"
            )
            sys.exit(0)

        comparison = compare_results(v2_results, v3_results)
        output = format_comparison(comparison)
        print(output)

    finally:
        db.close()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    main()

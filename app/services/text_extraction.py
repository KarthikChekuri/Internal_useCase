"""Text extraction service — Phase 2.1.

Dispatches extraction by file extension for .txt, .xlsx, .xls, and .csv files.
Returns the extracted text as a string on success, or None on any failure.
Never raises exceptions — all errors are caught internally and logged.
"""

import csv
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)


def extract_text(file_path: str) -> str | None:
    """Extract plain text content from a file based on its extension.

    Args:
        file_path: Absolute or relative path to the file.

    Returns:
        str on success (including empty string for empty files),
        None on failure (file not found, unsupported extension, corrupt file,
        encoding error).
    """
    try:
        path = Path(file_path)

        # File not found
        if not path.exists():
            logger.error("File not found: %s", file_path)
            return None

        # Dispatch by extension
        ext = path.suffix.lower()
        if ext == ".txt":
            return _extract_txt(path)
        elif ext == ".xlsx":
            return _extract_xlsx(path)
        elif ext == ".xls":
            return _extract_xls(path)
        elif ext == ".csv":
            return _extract_csv(path)
        else:
            logger.warning(
                "Unsupported file extension '%s' for file: %s", ext or "(none)", file_path
            )
            return None
    except Exception:
        logger.exception("Unexpected error extracting text from %s", file_path)
        return None


def _extract_txt(path: Path) -> str | None:
    """Read a plain text file as UTF-8."""
    try:
        text = path.read_text(encoding="utf-8")
        return text
    except (UnicodeDecodeError, ValueError) as exc:
        logger.warning("Encoding error reading %s: %s", path, exc)
        return None


def _extract_xlsx(path: Path) -> str | None:
    """Extract text from an Excel .xlsx file using openpyxl."""
    try:
        import openpyxl

        wb = openpyxl.load_workbook(str(path), read_only=True, data_only=True)
        parts: list[str] = []
        for ws in wb.worksheets:
            for row in ws.iter_rows():
                for cell in row:
                    if cell.value is not None:
                        parts.append(str(cell.value))
        wb.close()
        return " ".join(parts) if parts else ""
    except Exception as exc:
        logger.warning("Failed to read xlsx file %s: %s", path, exc)
        return None


def _extract_xls(path: Path) -> str | None:
    """Extract text from a legacy Excel .xls file using xlrd."""
    try:
        import xlrd

        wb = xlrd.open_workbook(str(path))
        parts: list[str] = []
        for sheet in wb.sheets():
            for row_idx in range(sheet.nrows):
                for col_idx in range(sheet.ncols):
                    value = sheet.cell_value(row_idx, col_idx)
                    if value is not None and value != "":
                        # xlrd returns floats for integers — convert cleanly
                        if isinstance(value, float) and value == int(value):
                            parts.append(str(int(value)))
                        else:
                            parts.append(str(value))
        return " ".join(parts) if parts else ""
    except Exception as exc:
        logger.warning("Failed to read xls file %s: %s", path, exc)
        return None


def _extract_csv(path: Path) -> str | None:
    """Extract text from a CSV file using the csv module."""
    try:
        with open(str(path), newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            parts: list[str] = []
            for row in reader:
                for cell in row:
                    if cell:
                        parts.append(cell)
        return " ".join(parts) if parts else ""
    except Exception as exc:
        logger.warning("Failed to read csv file %s: %s", path, exc)
        return None

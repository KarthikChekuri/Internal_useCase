"""Pydantic schemas for indexing operation responses.

Moved from app/services/indexing_service.py and updated with files_skipped
field to support resumable indexing (V2).
"""

from pydantic import BaseModel


class IndexResponse(BaseModel):
    """JSON-serializable response from indexing endpoints.

    Attributes:
        files_processed: Total number of files attempted (excludes skipped).
        files_succeeded: Number of files successfully indexed.
        files_failed: Number of files that failed extraction or upload.
        files_skipped: Number of files skipped (already indexed via resumability
                       or unsupported extension).
        errors: Error messages for each failed file (MD5 + reason).
    """

    files_processed: int
    files_succeeded: int
    files_failed: int
    files_skipped: int = 0
    errors: list[str] = []

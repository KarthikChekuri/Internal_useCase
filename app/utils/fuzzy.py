"""Fuzzy matching utilities for breach PII search.

Phase 2.3: Provides sliding window fuzzy matching, name normalization,
and name tokenization for the three-tier leak detection cascade.

Uses rapidfuzz.fuzz.token_set_ratio for fuzzy comparison, which naturally
handles reordered tokens (e.g., "Chekuri Karthik" matches "Karthik Chekuri").
"""

import logging
import re

from rapidfuzz.fuzz import token_set_ratio
from rapidfuzz.utils import default_process

logger = logging.getLogger(__name__)


def normalize_name(name: str) -> str:
    """Normalize a name for Tier 2 comparison.

    Lowercases, strips punctuation (apostrophes, hyphens, periods, commas,
    exclamation marks, etc.), replaces hyphens with spaces, and collapses
    whitespace.

    Args:
        name: The raw name string to normalize.

    Returns:
        Normalized name string, lowercased with punctuation removed and
        whitespace collapsed.
    """
    if not name:
        return ""

    result = name.lower()

    # Replace hyphens with spaces (so "Anne-Marie" becomes "anne marie")
    result = result.replace("-", " ")

    # Strip all other punctuation: apostrophes, periods, commas, etc.
    # Keep only alphanumeric characters and whitespace.
    result = re.sub(r"[^\w\s]", "", result)

    # Remove underscores (captured by \w but are punctuation for our purposes)
    result = result.replace("_", " ")

    # Collapse multiple whitespace into a single space and strip edges
    result = re.sub(r"\s+", " ", result).strip()

    return result


def tokenize_name(name: str) -> list[str]:
    """Tokenize a name into individual parts for matching.

    Normalizes the name first (lowercases, strips punctuation, replaces
    hyphens with spaces), then splits on whitespace.

    Args:
        name: The raw name string to tokenize.

    Returns:
        List of lowercase name tokens with punctuation removed.
        Empty list for empty input.
    """
    normalized = normalize_name(name)
    if not normalized:
        return []
    return normalized.split()


def sliding_window_fuzzy(
    text: str,
    search_term: str,
    threshold: int = 75,
) -> tuple[float, int]:
    """Compute fuzzy match score using a sliding window over text.

    Splits the text into overlapping windows of ``len(search_term) * 1.5``
    characters, computes ``token_set_ratio`` per window, and returns the
    maximum score and its position.

    The step size is ``max(1, len(search_term) // 2)`` (50% overlap between
    consecutive windows).

    Args:
        text: The full document/file text to search within.
        search_term: The PII value to search for (e.g., a name).
        threshold: Minimum score to consider a match (default 75).
            The function always returns the raw max score regardless of
            threshold -- the caller decides what to do with it.

    Returns:
        Tuple of (max_score, position) where:
        - max_score is the highest token_set_ratio across all windows (0.0-100.0)
        - position is the character offset of the window with the highest score
    """
    if not text or not search_term:
        return (0.0, 0)

    search_len = len(search_term)
    window_size = int(search_len * 1.5)
    step_size = max(1, search_len // 2)
    text_len = len(text)

    # If text is shorter than or equal to window size, compare entire text
    if text_len <= window_size:
        score = token_set_ratio(search_term, text, processor=default_process)
        return (float(score), 0)

    max_score = 0.0
    best_position = 0

    # Slide window across text
    pos = 0
    while pos <= text_len - window_size:
        window = text[pos : pos + window_size]
        score = token_set_ratio(search_term, window, processor=default_process)

        if score > max_score:
            max_score = score
            best_position = pos

        # Short-circuit: perfect score found
        if max_score == 100.0:
            break

        pos += step_size

    # Check the final tail window if we haven't covered the end
    # (and haven't already found a perfect match)
    if max_score < 100.0 and pos < text_len:
        tail_window = text[text_len - window_size :]
        score = token_set_ratio(search_term, tail_window, processor=default_process)
        if score > max_score:
            max_score = score
            best_position = text_len - window_size

    return (float(max_score), best_position)

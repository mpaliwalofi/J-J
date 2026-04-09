# utils/data_limiter.py
#
# Data Preprocessing & Summarization
#
# Prevents token overflow by intelligently reducing dataset size before sending to LLM.
# Strategies: aggregation, truncation, statistical summaries.

import logging
from typing import Any

logger = logging.getLogger(__name__)

# Token estimation: ~4 chars per token (conservative)
CHARS_PER_TOKEN = 4

# Safety limits
MAX_ROWS_FOR_LLM = 100  # Never send more than 100 rows to LLM
MAX_TOKENS_SAFE = 6000  # Safe limit for 8k context model (leave room for response)
TRUNCATE_THRESHOLD = 50  # Truncate if > 50 rows and not aggregated


def estimate_tokens(data: list[dict], context: list[dict] = None) -> int:
    """
    Estimate token count for data + context.

    Args:
        data: Query results
        context: KPI context items

    Returns:
        Estimated token count
    """
    # Convert data to string representation
    data_str = str(data) if data else ""
    context_str = str(context) if context else ""

    total_chars = len(data_str) + len(context_str)
    estimated_tokens = total_chars // CHARS_PER_TOKEN

    logger.info(
        "Token estimation: %d chars → ~%d tokens (data: %d rows, context: %d items)",
        total_chars, estimated_tokens, len(data) if data else 0,
        len(context) if context else 0
    )

    return estimated_tokens


def is_aggregated_result(data: list[dict]) -> bool:
    """
    Detect if result is already aggregated (AVG, SUM, COUNT, etc.).

    Heuristics:
    - Single row → likely aggregated
    - Column names contain: avg, sum, count, min, max, total
    - Small row count (< 20) → likely meaningful breakdown
    """
    if not data:
        return False

    # Single row is almost always an aggregation
    if len(data) == 1:
        return True

    # Check column names for aggregation indicators
    if data:
        headers = list(data[0].keys())
        agg_keywords = ['avg', 'sum', 'count', 'min', 'max', 'total', 'mean', 'median']

        for header in headers:
            header_lower = header.lower()
            if any(kw in header_lower for kw in agg_keywords):
                logger.info("Detected aggregated result (column: %s)", header)
                return True

    # Small result sets (< 20 rows) are likely meaningful breakdowns
    if len(data) < 20:
        return True

    return False


def summarize_large_dataset(data: list[dict]) -> dict:
    """
    Create statistical summary for large numeric datasets.

    Returns:
        {
            "summary_type": "statistical",
            "row_count": int,
            "columns": {...},
            "sample_rows": [...]
        }
    """
    if not data:
        return {"summary_type": "empty", "row_count": 0}

    row_count = len(data)
    headers = list(data[0].keys())

    # Compute statistics for numeric columns
    stats = {}
    for col in headers:
        values = []
        for row in data:
            val = row.get(col)
            if val is not None:
                # Try to parse as numeric
                try:
                    if isinstance(val, (int, float)):
                        values.append(float(val))
                    elif isinstance(val, str):
                        # Handle percentage strings
                        cleaned = val.replace('%', '').replace(',', '').strip()
                        values.append(float(cleaned))
                except (ValueError, AttributeError):
                    continue

        if values:
            stats[col] = {
                "count": len(values),
                "min": round(min(values), 2),
                "max": round(max(values), 2),
                "avg": round(sum(values) / len(values), 2)
            }
        else:
            # Count unique categorical values
            unique_vals = set(str(row.get(col, "")) for row in data if row.get(col) is not None)
            if len(unique_vals) < 50:  # Only show if reasonable number
                stats[col] = {
                    "type": "categorical",
                    "unique_count": len(unique_vals),
                    "sample_values": list(unique_vals)[:10]
                }

    return {
        "summary_type": "statistical",
        "row_count": row_count,
        "columns": stats,
        "sample_rows": data[:5]  # Include first 5 rows as examples
    }


def limit_data_for_llm(data: list[dict], query: str = "") -> tuple[list[dict], dict]:
    """
    Intelligently reduce data size before sending to LLM.

    Strategy:
    1. If already aggregated → keep as-is (up to MAX_ROWS_FOR_LLM)
    2. If > MAX_ROWS_FOR_LLM → return summary + sample
    3. If > TRUNCATE_THRESHOLD but < MAX_ROWS_FOR_LLM → truncate with warning

    Args:
        data: Query results
        query: Original user question (for context)

    Returns:
        (processed_data, metadata)
        - processed_data: Safe-sized data list
        - metadata: Info about what was done (truncated, summarized, etc.)
    """
    if not data:
        return data, {"action": "none", "original_rows": 0}

    original_count = len(data)

    # Check if already aggregated
    if is_aggregated_result(data):
        # Even aggregated results shouldn't be massive
        if original_count <= MAX_ROWS_FOR_LLM:
            logger.info("Data is aggregated (%d rows), passing through", original_count)
            return data, {"action": "none", "original_rows": original_count, "is_aggregated": True}
        else:
            # Rare case: aggregated but still too large (e.g., grouped by many categories)
            logger.warning("Aggregated result too large (%d rows), truncating to %d",
                          original_count, MAX_ROWS_FOR_LLM)
            return data[:MAX_ROWS_FOR_LLM], {
                "action": "truncated",
                "original_rows": original_count,
                "kept_rows": MAX_ROWS_FOR_LLM,
                "warning": f"Showing top {MAX_ROWS_FOR_LLM} of {original_count} results"
            }

    # Large dataset → create summary
    if original_count > MAX_ROWS_FOR_LLM:
        logger.warning(
            "Large dataset detected (%d rows). Creating statistical summary.",
            original_count
        )
        summary = summarize_large_dataset(data)

        # Return summary as a structured response that LLM can interpret
        summary_row = {
            "__SUMMARY__": f"Dataset too large ({original_count} rows). Statistical summary:",
            "row_count": original_count,
            "details": str(summary)
        }

        # Include sample rows
        sample_data = [summary_row] + data[:10]

        return sample_data, {
            "action": "summarized",
            "original_rows": original_count,
            "summary": summary,
            "warning": f"Summarized {original_count} rows → showing statistics + 10 samples"
        }

    # Medium dataset → truncate with warning
    if original_count > TRUNCATE_THRESHOLD:
        logger.info("Truncating dataset from %d to %d rows", original_count, TRUNCATE_THRESHOLD)
        return data[:TRUNCATE_THRESHOLD], {
            "action": "truncated",
            "original_rows": original_count,
            "kept_rows": TRUNCATE_THRESHOLD,
            "warning": f"Showing top {TRUNCATE_THRESHOLD} of {original_count} results"
        }

    # Small dataset → pass through
    logger.info("Dataset size OK (%d rows), passing through", original_count)
    return data, {"action": "none", "original_rows": original_count}


def ensure_token_safety(data: list[dict], context: list[dict]) -> tuple[list[dict], list[dict]]:
    """
    Final safety check: ensure combined data + context won't exceed token limit.

    If still too large, aggressively reduce both data and context.

    Returns:
        (safe_data, safe_context)
    """
    tokens = estimate_tokens(data, context)

    if tokens <= MAX_TOKENS_SAFE:
        logger.info("Token count safe: %d / %d", tokens, MAX_TOKENS_SAFE)
        return data, context

    logger.warning("Token count too high: %d / %d. Applying aggressive reduction.",
                   tokens, MAX_TOKENS_SAFE)

    # Reduce context first (easier to cut)
    reduced_context = context[:1] if context else []

    # Re-check
    tokens_after_context = estimate_tokens(data, reduced_context)
    if tokens_after_context <= MAX_TOKENS_SAFE:
        logger.info("Reduced context to 1 item. Tokens now: %d", tokens_after_context)
        return data, reduced_context

    # Still too large → aggressively truncate data
    max_data_rows = max(5, len(data) // 4)  # Keep at least 5 rows, or 25%
    reduced_data = data[:max_data_rows]

    logger.warning("Aggressively truncated data to %d rows", max_data_rows)
    return reduced_data, reduced_context

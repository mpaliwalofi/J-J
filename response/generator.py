import logging
import os
from groq import Groq
from utils.data_limiter import limit_data_for_llm, ensure_token_safety, estimate_tokens

logger = logging.getLogger(__name__)
client = Groq(api_key=os.getenv("GROQ_API_KEY"))

SYSTEM_PROMPT = """You are a supply chain KPI analyst for Johnson & Johnson.

You are given:
- The user's original question
- Query results from the PostgreSQL database (raw data rows)
- KPI definitions for business context ONLY

Output format rules — choose ONE based on the data shape:

1. TABLE (use when there are 2+ columns and 2+ rows — comparisons, breakdowns, rankings):
   Format as a clean markdown table with | headers | and | --- | separators.
   Include ALL rows provided (data may be pre-truncated for size).
   If you see a warning about truncated data, mention it briefly at the end.
   Example:
   | City    | Avg Utilisation (%) |
   |---------|---------------------|
   | Mumbai  | 78.5                |
   | Delhi   | 65.2                |
   **Summary:** Mumbai leads by 13.3 percentage points.

2. SINGLE VALUE (use when result is one number):
   Bold the value, then one sentence of context.
   Example: **94.3%** — OTIF rate for Q1 2024, above the 90% target.

3. LIST (use when data is a single column of values — e.g. city names, carrier names):
   Output as a numbered list with ALL values.
   Example:
   1. Mumbai
   2. Delhi
   3. Bangalore

4. NO DATA (use ONLY when the query results section says "No rows returned." i.e. 0 rows):
   State clearly: "No data found for [period/filter]. Try adjusting the time period or filters."
   NEVER say "no data" if even 1 row is present — show it instead.

Additional rules:
- Show ALL data provided (data is pre-filtered for size, so you should display everything given)
- If data contains a SUMMARY or STATISTICS row, interpret and explain it clearly
- Use KPI definitions only to explain business meaning — ignore any PQL/ARIS references in them
- Flag notable trends or anomalies only if clearly visible in the numbers
- Do NOT add caveats or suggestions unless rows are genuinely 0
"""


class ResponseGenerator:
    """
    LLM Service — Response Generation.
    Takes Data + Context from the MCP Layer and produces a natural language answer.
    """

    def generate(self, original_query: str, data: list[dict], context: list[dict], metadata: dict = None) -> str:
        """
        Generate natural language response from data.

        Args:
            original_query: User's question
            data: Query results (pre-limited for token safety)
            context: KPI definitions (pre-limited)
            metadata: Info about data preprocessing (truncation, summarization, etc.)
        """
        # Log initial sizes
        logger.info("Generating response for %d data rows, %d context items", len(data), len(context))

        # Apply intelligent data limiting
        limited_data, limit_metadata = limit_data_for_llm(data, original_query)
        logger.info("After limiting: %d rows (action: %s)", len(limited_data), limit_metadata.get("action"))

        # Final token safety check
        safe_data, safe_context = ensure_token_safety(limited_data, context)
        logger.info("After safety check: %d data rows, %d context items", len(safe_data), len(safe_context))

        # Estimate final token count
        final_tokens = estimate_tokens(safe_data, safe_context)
        logger.info("Final estimated tokens: %d", final_tokens)

        # Build data section
        data_section = f"Query Results ({len(safe_data)} rows"
        if limit_metadata.get("warning"):
            data_section += f" — {limit_metadata['warning']}"
        data_section += "):\n"

        if safe_data:
            # Format as aligned table so LLM sees clean columnar data
            headers = list(safe_data[0].keys())
            col_widths = {h: max(len(str(h)), max(len(str(row.get(h, ""))) for row in safe_data)) for h in headers}
            header_row = " | ".join(str(h).ljust(col_widths[h]) for h in headers)
            separator  = "-+-".join("-" * col_widths[h] for h in headers)
            rows       = [" | ".join(str(row.get(h, "")).ljust(col_widths[h]) for h in headers) for row in safe_data]
            data_section += "\n".join([header_row, separator] + rows)
        else:
            data_section += "No rows returned."

        # Build context section (reduced)
        context_section = ""
        if safe_context:
            lines = []
            for c in safe_context:
                name = c.get("KPI Name") or c.get("name", "")
                defn = c.get("Definition") or c.get("definition", "")
                if name or defn:
                    lines.append(f"- {name}: {defn}")
            if lines:
                context_section = "\nKPI Context:\n" + "\n".join(lines)

        # Generate response
        try:
            response = client.chat.completions.create(
                model="llama-3.1-8b-instant",
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {
                        "role": "user",
                        "content": (
                            f"Question: {original_query}\n\n"
                            f"{data_section}"
                            f"{context_section}"
                        ),
                    },
                ],
                max_tokens=2048,
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            logger.error("LLM response generation failed: %s", e)
            # Fallback: return basic summary if LLM fails
            if safe_data:
                return f"Retrieved {len(safe_data)} results. (LLM processing failed: {str(e)})"
            return "No data found."

# intent/classifier.py
#
# Intent Classifier — two backends:
#   1. DistilBERT (fine-tuned) — loaded from models/intent_classifier/
#      Auto-used when the model directory exists with a config.json inside.
#   2. Groq (Llama 3.3-70B) — API fallback when no trained model is present.
#
# Output format (always):
#   {
#     "intent":  str,          # one of the 7 INTENTS below
#     "metric":  str | None,   # e.g. "otif", "on_time_delivery"
#     "period":  str | None,   # e.g. "this_week", "last_month"
#     "filters": dict,         # e.g. {"region": "Mumbai"}
#     "confidence": float      # 0.0 – 1.0
#   }

from __future__ import annotations

import json
import logging
import os
import re
from pathlib import Path

logger = logging.getLogger(__name__)

# ── Constants ─────────────────────────────────────────────────────────────────

INTENTS = [
    "kpi_lookup",
    "trend_analysis",
    "comparison",
    "root_cause",
    "anomaly_detection",
    "forecast",
    "operational_status",
]

# Supply-chain metrics found in training data
_METRICS = {
    "otif":                   ["otif", "on time in full"],
    "on_time_delivery":       ["on.time delivery", "on-time delivery", "on time delivery", "delivery percentage", "delivery rate", "delivery performance"],
    "failed_deliveries":      ["failed deliver", "failed order", "delivery failure", "undelivered"],
    "sla_breach":             ["sla breach", "sla violation", "missed sla"],
    "warehouse_utilisation":  ["warehouse utilis", "warehouse utiliz", "warehouse capacity", "storage utiliz"],
    "load_rejection_rate":    ["load rejection", "rejection rate", "rejected load"],
    "transit_time":           ["transit time", "in transit", "transit duration"],
    "consolidation":          ["consolidation", "shipment consolidat"],
    "order_fill_rate":        ["order fill", "fill rate"],
    "avg_delay_duration":     ["delay duration", "average delay", "avg delay", "delay time"],
    "last_mile_cost":         ["last.mile cost", "last mile cost", "delivery cost", "cost per shipment"],
    "shipment_volume":        ["shipment volume", "number of shipment", "total shipment", "shipments"],
    "return_rate":            ["return rate", "returned", "returns"],
}

# Period keywords → canonical token
_PERIODS = {
    "today":         ["today", "this day"],
    "yesterday":     ["yesterday"],
    "this_week":     ["this week"],
    "last_week":     ["last week"],
    "last_7_days":   ["last 7 days", "past 7 days", "past week"],
    "this_month":    ["this month"],
    "last_month":    ["last month"],
    "last_30_days":  ["last 30 days", "past 30 days", "past month"],
    "this_quarter":  ["this quarter", "q1", "q2", "q3", "q4"],
    "last_quarter":  ["last quarter"],
    "last_90_days":  ["last 90 days", "past 90 days"],
    "this_year":     ["this year", "ytd", "year to date"],
    "last_6_months": ["last 6 months", "past 6 months", "past half year"],
    "last_12_months":["last 12 months", "past 12 months", "last year"],
}

# Specific year/quarter/month patterns
_YEAR_RE    = re.compile(r"\b(20\d{2})\b")
_QUARTER_RE = re.compile(r"\b[Qq]([1-4])\b")
_MONTH_NAMES = {
    "january": "01", "jan": "01",
    "february": "02", "feb": "02",
    "march": "03", "mar": "03",
    "april": "04", "apr": "04",
    "may": "05",
    "june": "06", "jun": "06",
    "july": "07", "jul": "07",
    "august": "08", "aug": "08",
    "september": "09", "sep": "09", "sept": "09",
    "october": "10", "oct": "10",
    "november": "11", "nov": "11",
    "december": "12", "dec": "12",
}

MODEL_DIR = Path(__file__).parent.parent / "models" / "intent_classifier"


# ── Entity extraction (rule-based, shared by both backends) ───────────────────

def _extract_entities(text: str) -> dict:
    """Extract metric, period, and filters from raw query text."""
    lower = text.lower()

    # Metric
    metric = None
    for key, patterns in _METRICS.items():
        if any(re.search(p, lower) for p in patterns):
            metric = key
            break

    # Period
    period = None
    for token, patterns in _PERIODS.items():
        if any(p in lower for p in patterns):
            period = token
            break

    # Year + month detection
    year_match = _YEAR_RE.search(lower)
    year = year_match.group(1) if year_match else None

    month = None
    for name, num in _MONTH_NAMES.items():
        if re.search(rf"\b{name}\b", lower):
            month = num
            break

    if period is None:
        if year and month:
            period = f"{year}-{month}"   # e.g. "2024-12"
        elif month:
            # no year mentioned — assume current year
            from datetime import date
            period = f"{date.today().year}-{month}"
        elif year:
            period = year               # e.g. "2024"

    # Quarter override (only if no finer period found)
    q_match = _QUARTER_RE.search(text)
    if q_match and period is None:
        period = f"q{q_match.group(1)}"

    # Filters — city and region names
    filters: dict = {}
    city_words = ["mumbai", "delhi", "bangalore", "chennai", "hyderabad", "kolkata", "pune"]
    region_words = ["north", "south", "east", "west", "central"]

    cities_found = [c.title() for c in city_words if c in lower]
    regions_found = [r.title() for r in region_words if r in lower]

    if cities_found:
        filters["cities"] = cities_found
    if regions_found:
        filters["regions"] = regions_found

    return {"metric": metric, "period": period, "filters": filters,
            "cities": cities_found, "regions": regions_found}


# ── Backend 1: Fine-tuned DistilBERT ─────────────────────────────────────────

class _DistilBERTClassifier:
    """Loads the fine-tuned DistilBERT model saved by classifier.ipynb."""

    def __init__(self, model_dir: Path):
        from transformers import pipeline as hf_pipeline
        self._pipe = hf_pipeline(
            "text-classification",
            model=str(model_dir),
            tokenizer=str(model_dir),
            device=-1,          # CPU
            truncation=True,
            max_length=64,
        )
        logger.info("Intent classifier: DistilBERT (fine-tuned) loaded from %s", model_dir)

    def classify(self, text: str) -> dict:
        result = self._pipe(text)[0]
        intent = result["label"]
        confidence = float(result["score"])
        entities = _extract_entities(text)
        return {
            "intent":     intent,
            "metric":     entities["metric"],
            "period":     entities["period"],
            "filters":    entities["filters"],
            "cities":     entities["cities"],
            "regions":    entities["regions"],
            "confidence": confidence,
        }


# ── Backend 2: Groq (Llama 3.3-70B) fallback ─────────────────────────────────

class _GroqClassifier:
    """Uses Groq API to classify intent when no trained model is available."""

    _SYSTEM = f"""You are an intent classifier for a supply chain analytics assistant.

Classify the user query into EXACTLY one of these intents:
{json.dumps(INTENTS, indent=2)}

Also extract:
- metric: the supply chain KPI being asked about (or null)
- period: the time period (or null)
- filters: any filters like region (empty object if none)

Respond ONLY with valid JSON in this exact format:
{{
  "intent": "<one of the intents above>",
  "metric": "<metric or null>",
  "period": "<period or null>",
  "filters": {{}}
}}"""

    def __init__(self):
        from groq import Groq
        self._client = Groq(api_key=os.environ["GROQ_API_KEY"])
        logger.info("Intent classifier: Groq (Llama 3.3-70B) — no trained model found")

    def classify(self, text: str) -> dict:
        response = self._client.chat.completions.create(
            model="llama-3.3-70b-versatile",
            messages=[
                {"role": "system", "content": self._SYSTEM},
                {"role": "user",   "content": text},
            ],
            temperature=0.0,
            max_tokens=200,
        )
        raw = response.choices[0].message.content.strip()

        # Strip markdown code fences if present
        raw = re.sub(r"^```(?:json)?\s*", "", raw)
        raw = re.sub(r"\s*```$", "", raw)

        parsed = json.loads(raw)

        # Validate intent
        intent = parsed.get("intent", "kpi_lookup")
        if intent not in INTENTS:
            intent = "kpi_lookup"

        # Rule-based entity extraction enriches / overrides LLM output
        entities = _extract_entities(text)
        metric  = entities["metric"]  or parsed.get("metric")
        period  = entities["period"]  or parsed.get("period")
        filters = entities["filters"] or parsed.get("filters", {})

        return {
            "intent":     intent,
            "metric":     metric,
            "period":     period,
            "filters":    filters,
            "cities":     entities["cities"],
            "regions":    entities["regions"],
            "confidence": 0.9,   # Groq doesn't return confidence
        }


# ── Public API ────────────────────────────────────────────────────────────────

class IntentClassifier:
    """
    Auto-selects backend:
      - DistilBERT if models/intent_classifier/config.json exists
      - Groq otherwise
    """

    def __init__(self):
        config_path = MODEL_DIR / "config.json"
        if config_path.exists():
            self._backend = _DistilBERTClassifier(MODEL_DIR)
        else:
            self._backend = _GroqClassifier()

    def classify(self, text: str) -> dict:
        """
        Classify a raw natural language query.

        Returns:
            {
              "intent":     str,
              "metric":     str | None,
              "period":     str | None,
              "filters":    dict,
              "confidence": float
            }

        Raises:
            ValueError: if metric or period could not be extracted from the query.
        """
        result = self._backend.classify(text)
        logger.info(
            "Intent: %s | metric: %s | period: %s | confidence: %.2f",
            result["intent"], result["metric"], result["period"], result["confidence"],
        )

        # Exploratory / schema questions don't need metric or period — let them pass
        _EXPLORATORY = re.compile(
            r"\b(what|which|list|show|display|give me|tell me|are there|how many)\b"
            r".{0,60}"
            r"\b(cities|regions|carriers|routes|plants|warehouses|tables|columns|"
            r"available|stored|exist|options|values|records|data)\b",
            re.IGNORECASE,
        )
        if _EXPLORATORY.search(text):
            return result

        missing = []
        if result["metric"] is None:
            missing.append("metric (e.g. 'warehouse utilisation', 'OTIF', 'lead time')")
        if result["period"] is None:
            missing.append("time period (e.g. '2024', 'December 2024', 'Q1 2024', 'January 2024')")

        if missing:
            # Build a dynamic suggestion using what the user already provided
            metric_example = result["metric"] or "warehouse utilisation"
            cities         = result.get("cities", [])
            regions        = result.get("regions", [])

            location_phrase = ""
            if len(cities) >= 2:
                location_phrase = f" between {cities[0]} and {cities[1]}"
            elif len(cities) == 1:
                location_phrase = f" in {cities[0]}"
            elif len(regions) >= 2:
                location_phrase = f" between {regions[0]} and {regions[1]}"
            elif len(regions) == 1:
                location_phrase = f" in {regions[0]}"

            # Suggest what's missing based on what's already in the query
            if result["metric"] is None and result["period"] is None:
                period_suggestion = "2024"
                metric_display = "warehouse utilisation"
            elif result["metric"] is None:
                period_suggestion = result["period"]
                metric_display = "warehouse utilisation"
            else:
                period_suggestion = "2024"
                metric_display = metric_example.replace("_", " ")

            # Use "for" with relative periods, "in" with specific months/years
            preposition = "for" if period_suggestion in (
                "last month", "this month", "last week", "this week",
                "last quarter", "this quarter", "this year", "last year",
                "last_month", "this_month", "last_week", "this_week",
            ) else "in"
            example = f"'{metric_display.capitalize()}{location_phrase} {preposition} {period_suggestion}'"

            raise ValueError(
                f"Incomplete query — could not determine: {', '.join(missing)}.\n"
                f"Please include both a KPI and a time period. For example:\n"
                f"  {example}"
            )

        return result

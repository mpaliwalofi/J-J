# tests/test_classifier.py
#
# Unit tests for intent/classifier.py — _extract_entities() and IntentClassifier.classify()
# Tests the rule-based extraction layer (no API/model calls needed for entity tests).

import pytest
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from intent.classifier import _extract_entities, IntentClassifier


class TestExtractEntities:

    # ── Metric extraction ─────────────────────────────────────────────────────

    def test_otif_metric(self):
        r = _extract_entities("What is the OTIF rate this week?")
        assert r["metric"] == "otif"

    def test_warehouse_utilisation_metric(self):
        r = _extract_entities("Compare warehouse utilisation between Mumbai and Delhi")
        assert r["metric"] == "warehouse_utilisation"

    def test_on_time_delivery_metric(self):
        r = _extract_entities("Show me on-time delivery rate last month")
        assert r["metric"] == "on_time_delivery"

    def test_transit_time_metric(self):
        r = _extract_entities("What is the transit time this month?")
        assert r["metric"] == "transit_time"

    def test_no_metric_returns_none(self):
        r = _extract_entities("Show me data for last month")
        assert r["metric"] is None

    # ── Period extraction — keywords ──────────────────────────────────────────

    def test_this_week(self):
        r = _extract_entities("What is OTIF this week?")
        assert r["period"] == "this_week"

    def test_last_month(self):
        r = _extract_entities("Show warehouse utilisation last month")
        assert r["period"] == "last_month"

    def test_last_30_days(self):
        r = _extract_entities("Show trend over last 30 days")
        assert r["period"] == "last_30_days"

    def test_this_year(self):
        r = _extract_entities("What is the OTIF rate this year?")
        assert r["period"] == "this_year"

    # ── Period extraction — year + month ──────────────────────────────────────

    def test_month_and_year(self):
        r = _extract_entities("Compare warehouse utilisation in December 2024")
        assert r["period"] == "2024-12"

    def test_month_abbreviation_and_year(self):
        r = _extract_entities("What is lead time in Jan 2024?")
        assert r["period"] == "2024-01"

    def test_year_only(self):
        r = _extract_entities("Show savings lost in 2024")
        assert r["period"] == "2024"

    def test_month_only_uses_current_year(self):
        from datetime import date
        r = _extract_entities("What is OTIF in March?")
        assert r["period"] == f"{date.today().year}-03"

    # ── City extraction ───────────────────────────────────────────────────────

    def test_two_cities(self):
        r = _extract_entities("Compare utilisation between Mumbai and Delhi")
        assert "Mumbai" in r["cities"]
        assert "Delhi" in r["cities"]

    def test_single_city(self):
        r = _extract_entities("Show OTIF in Chennai last month")
        assert r["cities"] == ["Chennai"]

    def test_no_city(self):
        r = _extract_entities("What is OTIF this week?")
        assert r["cities"] == []

    # ── Region extraction ─────────────────────────────────────────────────────

    def test_region_north_south(self):
        r = _extract_entities("Compare lead time between North and South last month")
        assert "North" in r["regions"]
        assert "South" in r["regions"]

    def test_no_region(self):
        r = _extract_entities("What is OTIF in Mumbai this month?")
        assert r["regions"] == []


class TestIntentClassifierValidation:
    """
    Tests for the validation logic in IntentClassifier.classify().
    Uses the DistilBERT or Groq backend — skips if no model or API key available.
    """

    @pytest.fixture(scope="class")
    def classifier(self):
        try:
            return IntentClassifier()
        except Exception as e:
            pytest.skip(f"Classifier could not be initialised: {e}")

    def test_complete_query_passes(self, classifier):
        result = classifier.classify("What is the OTIF rate last month?")
        assert result["metric"] is not None
        assert result["period"] is not None
        assert "intent" in result
        assert "confidence" in result

    def test_missing_period_raises(self, classifier):
        with pytest.raises(ValueError, match="time period"):
            classifier.classify("What is warehouse utilisation in Mumbai?")

    def test_missing_metric_raises(self, classifier):
        # "Show me data for last month" matches the exploratory pattern, use a non-exploratory query
        with pytest.raises(ValueError, match="metric"):
            classifier.classify("Show me numbers for last month in North region")

    def test_exploratory_query_bypasses_validation(self, classifier):
        # Should NOT raise even though metric and period are None
        result = classifier.classify("What cities are stored in the database?")
        assert result is not None

    def test_list_carriers_bypasses_validation(self, classifier):
        result = classifier.classify("List all carriers available")
        assert result is not None

    def test_cities_populated(self, classifier):
        result = classifier.classify("Compare warehouse utilisation between Mumbai and Delhi in December 2024")
        assert "Mumbai" in result["cities"]
        assert "Delhi" in result["cities"]

    def test_period_year_month(self, classifier):
        result = classifier.classify("What is the transit time in January 2024?")
        assert result["period"] == "2024-01"

    def test_error_message_contains_user_metric(self, classifier):
        # When metric is detected but period is missing, example should use their metric
        try:
            classifier.classify("What is OTIF in Mumbai?")
        except ValueError as e:
            assert "otif" in str(e).lower() or "OTIF" in str(e)

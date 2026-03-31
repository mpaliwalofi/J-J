# tests/test_response_generator.py
#
# Unit tests for response/generator.py
# Groq API is mocked — no live API calls needed.

import pytest
import sys
import os
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from response.generator import ResponseGenerator


SAMPLE_DATA = [
    {"Source City": "Mumbai",    "avg_warehouse_utilisation": 78.5},
    {"Source City": "Delhi",     "avg_warehouse_utilisation": 65.2},
    {"Source City": "Bangalore", "avg_warehouse_utilisation": 71.0},
]

SAMPLE_CONTEXT = [
    {"KPI Name": "Warehouse Utilisation", "Definition": "Daily shift capacity utilisation percentage."},
]


def _make_generator(reply: str = "Mumbai: 78.5%, Delhi: 65.2%") -> ResponseGenerator:
    gen = ResponseGenerator()
    mock_response = MagicMock()
    mock_response.choices[0].message.content = reply
    with patch("response.generator.client") as mock_client:
        mock_client.chat.completions.create.return_value = mock_response
        gen._mock_client = mock_client
    return gen


class TestResponseGenerator:

    def test_generate_returns_string(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "Mumbai utilisation is 78.5%."

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            result = gen.generate(
                "Compare warehouse utilisation between Mumbai and Delhi",
                SAMPLE_DATA,
                SAMPLE_CONTEXT,
            )

        assert isinstance(result, str)
        assert len(result) > 0

    def test_generate_sends_all_rows(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "answer"

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            gen.generate("test query", SAMPLE_DATA, SAMPLE_CONTEXT)
            call_args = mock_client.chat.completions.create.call_args
            user_content = call_args[1]["messages"][1]["content"]

        # All 3 cities must appear in the data sent to the LLM
        assert "Mumbai" in user_content
        assert "Delhi" in user_content
        assert "Bangalore" in user_content

    def test_generate_sends_column_headers(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "answer"

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            gen.generate("test query", SAMPLE_DATA, SAMPLE_CONTEXT)
            call_args = mock_client.chat.completions.create.call_args
            user_content = call_args[1]["messages"][1]["content"]

        assert "Source City" in user_content
        assert "avg_warehouse_utilisation" in user_content

    def test_generate_includes_kpi_context(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "answer"

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            gen.generate("test query", SAMPLE_DATA, SAMPLE_CONTEXT)
            call_args = mock_client.chat.completions.create.call_args
            user_content = call_args[1]["messages"][1]["content"]

        assert "Warehouse Utilisation" in user_content

    def test_generate_handles_empty_data(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "No data available."

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            result = gen.generate("test query", [], [])

        assert isinstance(result, str)

    def test_generate_handles_empty_context(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "Mumbai: 78.5%"

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            result = gen.generate("test query", SAMPLE_DATA, [])

        assert isinstance(result, str)

    def test_generate_uses_2048_max_tokens(self):
        gen = ResponseGenerator()
        mock_response = MagicMock()
        mock_response.choices[0].message.content = "answer"

        with patch("response.generator.client") as mock_client:
            mock_client.chat.completions.create.return_value = mock_response
            gen.generate("test", SAMPLE_DATA, SAMPLE_CONTEXT)
            call_args = mock_client.chat.completions.create.call_args
            assert call_args[1]["max_tokens"] == 2048

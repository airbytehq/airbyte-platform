import os
import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch

from app.main import app

client = TestClient(app)


class TestCapabilities:
    """Test cases for the capabilities endpoint."""

    def test_capabilities_endpoint_exists(self):
        """Test that the capabilities endpoint is accessible."""
        response = client.get("/capabilities/")
        assert response.status_code == 200

    def test_capabilities_custom_code_execution_false_by_default(self):
        """Test that customCodeExecution is false by default when env var is not set."""
        with patch.dict(os.environ, {}, clear=True):
            response = client.get("/capabilities/")
            assert response.status_code == 200

            data = response.json()
            assert "customCodeExecution" in data
            assert data["customCodeExecution"] is False

    def test_capabilities_custom_code_execution_false_when_env_var_false(self):
        """Test that customCodeExecution is false when env var is explicitly set to false."""
        with patch.dict(os.environ, {"AIRBYTE_ENABLE_UNSAFE_CODE": "false"}):
            response = client.get("/capabilities/")
            assert response.status_code == 200

            data = response.json()
            assert "customCodeExecution" in data
            assert data["customCodeExecution"] is False

    def test_capabilities_custom_code_execution_true_when_env_var_true(self):
        """Test that customCodeExecution is true when env var is set to true."""
        with patch.dict(os.environ, {"AIRBYTE_ENABLE_UNSAFE_CODE": "true"}):
            response = client.get("/capabilities/")
            assert response.status_code == 200

            data = response.json()
            assert "customCodeExecution" in data
            assert data["customCodeExecution"] is True

    def test_capabilities_custom_code_execution_case_insensitive(self):
        """Test that env var parsing is case insensitive."""
        test_cases = ["TRUE", "True", "tRuE"]

        for value in test_cases:
            with patch.dict(os.environ, {"AIRBYTE_ENABLE_UNSAFE_CODE": value}):
                response = client.get("/capabilities/")
                assert response.status_code == 200

                data = response.json()
                assert data["customCodeExecution"] is True

    def test_capabilities_custom_code_execution_invalid_values_default_to_false(self):
        """Test that invalid env var values default to false."""
        invalid_values = ["yes", "1", "on", "enabled", "invalid"]

        for value in invalid_values:
            with patch.dict(os.environ, {"AIRBYTE_ENABLE_UNSAFE_CODE": value}):
                response = client.get("/capabilities/")
                assert response.status_code == 200

                data = response.json()
                assert data["customCodeExecution"] is False

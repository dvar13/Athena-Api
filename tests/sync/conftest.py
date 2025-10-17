import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

@pytest.fixture(scope="session", autouse=True)
def setup_boto3_mock_before_import():
    """
    Este fixture se ejecuta antes de importar main.py,
    asegurando que el mock reemplace boto3.client globalmente.
    """
    mock_athena = MagicMock()

    mock_athena.start_query_execution.return_value = {
        "QueryExecutionId": "mocked-query-123"
    }
    mock_athena.get_query_execution.return_value = {
        "QueryExecution": {"Status": {"State": "SUCCEEDED"}}
    }
    mock_athena.get_query_results.return_value = {
        "ResultSet": {
            "ResultSetMetadata": {
                "ColumnInfo": [
                    {"Label": "col1"},
                    {"Label": "col2"},
                ]
            },
            "Rows": [
                {"Data": [{"VarCharValue": "col1"}, {"VarCharValue": "col2"}]},
                {"Data": [{"VarCharValue": "val1"}, {"VarCharValue": "val2"}]},
            ],
        }
    }

    with patch("boto3.client", return_value=mock_athena):
        yield

@pytest.fixture
def client():
    # Importamos aquí después del patch
    from main import app
    return TestClient(app)

import os
import boto3
import pytest
import pytest_asyncio
from httpx import AsyncClient, ASGITransport
from fastapi.testclient import TestClient
from dotenv import load_dotenv
from main import app

# Cargar .env
load_dotenv()

@pytest.fixture(scope="session")
def aws_config():
    """Configura el cliente real de Athena usando .env o secretos del entorno."""
    required_vars = ["ATHENA_DATABASE", "ATHENA_OUTPUT", "AWS_REGION"]
    missing = [v for v in required_vars if not os.getenv(v)]

    if missing:
        pytest.skip(f"❌ Variables faltantes para pruebas reales: {missing}")

    client = boto3.client("athena", region_name=os.getenv("AWS_REGION"))
    return {
        "athena": client,
        "database": os.getenv("ATHENA_DATABASE"),
        "output": os.getenv("ATHENA_OUTPUT"),
    }

@pytest.fixture(scope="session")
def client():
    """Cliente HTTP síncrono (para pruebas sync)."""
    return TestClient(app)

@pytest_asyncio.fixture
async def async_client():
    base_url = os.getenv("API_BASE_URL")
    if not base_url:
        pytest.skip("❌ Variable API_BASE_URL no definida para tests async reales")
    
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url=base_url) as client:
        yield client

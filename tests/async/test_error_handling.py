import pytest

@pytest.mark.asyncio
async def test_invalid_table(async_client):
    response = await async_client.get("/table/invalid_table")
    assert response.status_code == 400
    assert "Tabla" in response.json()["detail"]

@pytest.mark.asyncio
async def test_invalid_query(async_client):
    q = "SELECT * FROM nonexistent_table;"
    response = await async_client.get("/query", params={"q": q})
    assert response.status_code in (400, 500)

import pytest

@pytest.mark.asyncio
async def test_async_tables(async_client):
    response = await async_client.get("/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert isinstance(data["tables"], list)

@pytest.mark.asyncio
async def test_async_query(async_client):
    q = "SELECT * FROM dim_date LIMIT 1;"
    response = await async_client.get("/query", params={"q": q})
    assert response.status_code == 200
    json_data = response.json()
    assert "rows" in json_data

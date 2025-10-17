def test_query_valid(client):
    q = "SELECT * FROM dim_date LIMIT 3;"
    response = client.get("/query", params={"q": q})
    assert response.status_code == 200
    data = response.json()
    assert "rows" in data
    assert isinstance(data["rows"], list)

def test_query_missing_param(client):
    response = client.get("/query")
    assert response.status_code == 422  # parámetro faltante

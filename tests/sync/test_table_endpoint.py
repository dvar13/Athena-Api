def test_get_table_valid(client):
    response = client.get("/table/dim_date?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert "rows" in data
    assert isinstance(data["rows"], list)

def test_get_table_invalid(client):
    response = client.get("/table/invalid_table")
    assert response.status_code == 400

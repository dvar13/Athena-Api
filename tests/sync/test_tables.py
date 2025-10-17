def test_list_tables(client):
    response = client.get("/tables")
    assert response.status_code == 200
    data = response.json()
    assert "tables" in data
    assert isinstance(data["tables"], list)
    assert "dim_date" in data["tables"]

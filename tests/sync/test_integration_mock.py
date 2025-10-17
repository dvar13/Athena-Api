def test_integration_tables_and_query(client):
    tables = client.get("/tables")
    assert tables.status_code == 200
    first_table = tables.json()["tables"][0]

    query = f"SELECT * FROM {first_table} LIMIT 1;"
    response = client.get("/query", params={"q": query})
    assert response.status_code == 200

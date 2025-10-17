import pytest

def test_real_query_to_athena(client):
    """Verifica que Athena responda correctamente usando credenciales reales."""
    q = "SELECT * FROM dim_date LIMIT 3;"
    response = client.get("/query", params={"q": q})

    assert response.status_code == 200, response.text
    data = response.json()
    assert "rows" in data
    assert len(data["rows"]) > 0


def test_real_table_endpoint(client):
    """Verifica el endpoint de tabla real."""
    response = client.get("/table/dim_customer?limit=3")
    assert response.status_code == 200
    json_data = response.json()
    assert "rows" in json_data
    assert len(json_data["rows"]) <= 3

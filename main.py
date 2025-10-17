import os
import time
import boto3
import pandas as pd
from fastapi import FastAPI, Query, HTTPException
from dotenv import load_dotenv

# Cargar variables desde .env
load_dotenv()

# Inicializar FastAPI
app = FastAPI(title="API Athena - Sakila DW", version="1.0")

# Configuración AWS
ATHENA_DATABASE = os.getenv("ATHENA_DATABASE")
ATHENA_OUTPUT = os.getenv("ATHENA_OUTPUT")
REGION = os.getenv("AWS_REGION")

client = boto3.client("athena", region_name=REGION)

# ---- Función auxiliar para ejecutar consultas ----
def run_athena_query(query: str):
    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
    )
    query_id = response["QueryExecutionId"]

    # Esperar a que la consulta termine
    while True:
        result = client.get_query_execution(QueryExecutionId=query_id)
        status = result["QueryExecution"]["Status"]["State"]
        if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
            break
        time.sleep(1)

    if status != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get("StateChangeReason", "Unknown")
        raise Exception(f"Query failed: {status} ({reason})")

    # Obtener resultados
    result = client.get_query_results(QueryExecutionId=query_id)

    columns = [col["Label"] for col in result["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]]
    rows = []
    for row in result["ResultSet"]["Rows"][1:]:  # omitir encabezado
        rows.append([field.get("VarCharValue", None) for field in row["Data"]])

    df = pd.DataFrame(rows, columns=columns)
    return df.to_dict(orient="records")

# ---- Endpoint: listar tablas disponibles ----
@app.get("/tables")
def list_tables():
    tables = ["dim_customer", "dim_date", "dim_film", "dim_store", "fact_rental"]
    return {"database": ATHENA_DATABASE, "tables": tables}

# ---- Endpoint: consultar cualquier tabla ----
@app.get("/table/{table_name}")
def get_table(table_name: str, limit: int = 20):
    allowed_tables = ["dim_customer", "dim_date", "dim_film", "dim_store", "fact_rental"]

    if table_name not in allowed_tables:
        raise HTTPException(status_code=400, detail=f"Tabla '{table_name}' no permitida")

    query = f"SELECT * FROM {table_name} LIMIT {limit};"
    try:
        results = run_athena_query(query)
        return {"table": table_name, "count": len(results), "rows": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ---- Endpoint: ejecutar consultas SQL personalizadas ----
@app.get("/query")
def execute_query(q: str = Query(..., description="Consulta SQL para ejecutar en Athena")):
    try:
        results = run_athena_query(q)
        return {"query": q, "count": len(results), "rows": results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# ==========================================================
#               ENDPOINTS DE VISUALIZACIÓN
# ==========================================================

# Gráfico de barras: número de alquileres por día de la semana
@app.get("/charts/rentals-by-day")
def rentals_by_day():
    query = """
    SELECT 
        d.day_of_week,
        COUNT(f.rental_date) AS total_rentals
    FROM fact_rental f
    JOIN dim_date d 
        ON f.rental_date = d.rental_date
    GROUP BY d.day_of_week
    ORDER BY 
        CASE 
            WHEN d.day_of_week = 'Monday' THEN 1
            WHEN d.day_of_week = 'Tuesday' THEN 2
            WHEN d.day_of_week = 'Wednesday' THEN 3
            WHEN d.day_of_week = 'Thursday' THEN 4
            WHEN d.day_of_week = 'Friday' THEN 5
            WHEN d.day_of_week = 'Saturday' THEN 6
            WHEN d.day_of_week = 'Sunday' THEN 7
        END;
    """
    return {"chart": "rentals_by_day", "data": run_athena_query(query)}
    

# Gráfico temporal: número de alquileres en junio 2006
@app.get("/charts/rentals-june-2006")
def rentals_june_2006():
    query = """
    SELECT 
        f.rental_date,
        COUNT(*) AS total_rentals
    FROM fact_rental f
    WHERE f.rental_date BETWEEN DATE('2006-06-01') AND DATE('2006-06-30')
    GROUP BY f.rental_date
    ORDER BY f.rental_date;
    """
    return {"chart": "rentals_june_2006", "data": run_athena_query(query)}




# Gráfico de torta: porcentaje de alquileres por tienda
@app.get("/charts/rentals-by-store")
def rentals_by_store():
    query = """
    SELECT 
        s.store_id,
        COUNT(f.rental_date) AS total_rentals,
        ROUND((COUNT(f.rental_date) * 100.0 / SUM(COUNT(f.rental_date)) OVER()), 2) AS percentage
    FROM fact_rental f
    JOIN dim_store s 
        ON f.store_id = s.store_id
    GROUP BY s.store_id;
    """
    return {"chart": "rentals_by_store", "data": run_athena_query(query)}

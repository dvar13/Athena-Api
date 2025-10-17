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

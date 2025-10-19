import pytest
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from etl_dim_date import bucket_input, prefix_input, bucket_output, prefix_output

@pytest.mark.usefixtures("mock_s3")
def test_etl_dim_date(mock_s3, monkeypatch):
    # Reemplazar el cliente real por el mock
    import etl_dim_date
    etl_dim_date.s3 = mock_s3

    # Ejecutar el script
    etl_dim_date.main() if hasattr(etl_dim_date, "main") else None

    # Leer el resultado simulado
    output_key = f"{prefix_output}dim_date.snappy.parquet"
    response = mock_s3.get_object(Bucket=bucket_output, Key=output_key)
    buf = BytesIO(response["Body"].read())
    df = pq.read_table(buf).to_pandas()

    # --- Validaciones ---
    # 1. Validar que rental_date es fecha y contiene 2005-05-24
    assert pd.to_datetime(df["rental_date"]).dt.strftime("%Y-%m-%d").isin(["2005-05-24"]).any()

    # 2. Validar formato del date_id
    assert df["date_id"].astype(str).str.match(r"^\d{8}$").all()

    # 3. Validar columnas esperadas
    expected_cols = {
        "rental_date", "date_id", "day", "month", "year",
        "day_of_week", "week_of_year", "quarter",
        "is_weekend", "is_holiday"
    }
    assert set(df.columns) == expected_cols

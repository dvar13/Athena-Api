import pytest
import boto3
from moto import mock_aws
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq

@pytest.fixture
def mock_s3():
    with mock_aws():
        s3 = boto3.client("s3", region_name="us-east-1")
        bucket = "sakila-rds-customers"
        s3.create_bucket(Bucket=bucket)

        # Crear DataFrame simulado
        df = pd.DataFrame({
            "rental_date": ["2005-05-24", "2005-05-25", "2005-05-26"],
            "amount": [1, 2, 3]
        })

        # Guardar como parquet
        table = pa.Table.from_pandas(df)
        buf = BytesIO()
        pq.write_table(table, buf)
        buf.seek(0)

        # Subir archivo simulado
        s3.put_object(Bucket=bucket, Key="fact_rental/mock_file.parquet", Body=buf.read())

        yield s3

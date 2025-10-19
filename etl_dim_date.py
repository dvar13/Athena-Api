import boto3
import pandas as pd
import holidays
from io import BytesIO
import pyarrow.parquet as pq
import pyarrow as pa

# --- CONFIGURACIÓN ---
s3 = boto3.client('s3')

bucket_input = 'sakila-rds-customers'
prefix_input = 'fact_rental/'   # carpeta de entrada con los .parquet
bucket_output = 'sakila-rds-customers'
prefix_output = 'dim_date/'     # carpeta destino para guardar la tabla


def main():
    
    # --- LISTAR ARCHIVOS PARQUET EN S3 ---
    response = s3.list_objects_v2(Bucket=bucket_input, Prefix=prefix_input)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet')]
    
    if not files:
        raise Exception(f"No se encontraron archivos .parquet en s3://{bucket_input}/{prefix_input}")
    
    # --- LEER PARQUETS Y COMBINAR ---
    dfs = []
    for file_key in files:
        print(f"Leyendo {file_key} ...")
        obj = s3.get_object(Bucket=bucket_input, Key=file_key)
        buffer = BytesIO(obj['Body'].read())
        table = pq.read_table(buffer)
        df = table.to_pandas()
        dfs.append(df)
    
    fact_rental_df = pd.concat(dfs, ignore_index=True)
    
    # --- LIMPIEZA Y TRANSFORMACIÓN ---
    # Aseguramos que rental_date sea datetime y solo se use la parte de fecha
    fact_rental_df['rental_date'] = pd.to_datetime(fact_rental_df['rental_date']).dt.date
    
    # Crear DataFrame con todas las fechas únicas presentes en fact_rental
    # (No se pierden registros; solo se crea una fila por fecha distinta)
    unique_dates = sorted(fact_rental_df['rental_date'].unique())
    dates_df = pd.DataFrame({'rental_date': unique_dates})
    
    # --- GENERAR COLUMNAS DE DIMENSIÓN ---
    dates_df['date_id'] = pd.to_datetime(dates_df['rental_date']).dt.strftime('%Y%m%d').astype(int)
    dates_df['day'] = pd.to_datetime(dates_df['rental_date']).dt.day
    dates_df['month'] = pd.to_datetime(dates_df['rental_date']).dt.month
    dates_df['year'] = pd.to_datetime(dates_df['rental_date']).dt.year
    dates_df['day_of_week'] = pd.to_datetime(dates_df['rental_date']).dt.day_name()
    dates_df['week_of_year'] = pd.to_datetime(dates_df['rental_date']).dt.isocalendar().week.astype(int)
    dates_df['quarter'] = pd.to_datetime(dates_df['rental_date']).dt.quarter
    dates_df['is_weekend'] = pd.to_datetime(dates_df['rental_date']).dt.dayofweek.isin([5, 6])
    
    # --- FERIADOS EN EE.UU. ---
    us_holidays = holidays.US()
    dates_df['is_holiday'] = dates_df['rental_date'].isin(us_holidays)
    
    # --- GUARDAR RESULTADO COMO PARQUET SNAPPY ---
    table = pa.Table.from_pandas(dates_df, preserve_index=False)
    buffer_out = BytesIO()
    pq.write_table(table, buffer_out, compression='snappy')
    
    output_key = f"{prefix_output}dim_date.snappy.parquet"
    s3.put_object(Bucket=bucket_output, Key=output_key, Body=buffer_out.getvalue())
    
    print("ETL completado correctamente.")
    print(f"Archivo generado: s3://{bucket_output}/{output_key}")

if __name__ == "__main__":
    main()
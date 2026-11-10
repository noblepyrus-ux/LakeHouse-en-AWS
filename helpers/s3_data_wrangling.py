import boto3
import pandas as pd
from io import StringIO, BytesIO

def read_csv_from_s3(bucket, key):
    """Lee un CSV desde S3 y retorna un DataFrame"""
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(obj['Body'])

def write_parquet_to_s3(df, bucket, key):
    """Escribe un DataFrame como Parquet en S3"""
    s3 = boto3.client('s3')
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=buffer.getvalue())

def validate_schema(df, required_columns):
    """Valida que el DataFrame tenga las columnas requeridas"""
    missing = set(required_columns) - set(df.columns)
    if missing:
        raise ValueError(f"Columnas faltantes: {missing}")
    return True

def clean_column_names(df):
    """Limpia nombres de columnas (minúsculas, sin espacios)"""
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    return df
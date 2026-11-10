"""
JOB STAGING: Convierte CSV de Landing a Parquet en Bronze
Tipo: Python Shell (Glue)
Entrada: S3 lakehouse-andres-2026-landing/raw_data.csv
Salida:  S3 lakehouse-andres-2026-bronze/bronze_data.parquet
"""
import sys
import os
import boto3
import pandas as pd
from io import BytesIO

# Agregar helpers al path (Glue Python Shell lo permite)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from helpers.s3_data_wrangling import (
    read_csv_from_s3,
    write_parquet_to_s3,
    validate_schema,
    clean_column_names
)

# Configuración
LANDING_BUCKET = 'lakehouse-andres-2026-landing'
LANDING_KEY = 'raw_data.csv'
BRONZE_BUCKET = 'lakehouse-andres-2026-bronze'
BRONZE_KEY = 'bronze_data.parquet'

REQUIRED_COLUMNS = ['invoice_id', 'stock_code', 'description', 
                    'quantity', 'invoice_date', 'price', 
                    'customer_id', 'country', 'department', 'age']

def main():
    print("=" * 60)
    print("JOB STAGING: CSV -> Parquet (Bronze)")
    print("=" * 60)
    
    # 1. Leer CSV desde S3
    print(f"[1/4] Leyendo CSV desde s3://{LANDING_BUCKET}/{LANDING_KEY}...")
    df = read_csv_from_s3(LANDING_BUCKET, LANDING_KEY)
    print(f"      Filas leídas: {len(df)}")
    
    # 2. Limpiar nombres de columnas
    print("[2/4] Limpiando nombres de columnas...")
    df = clean_column_names(df)
    
    # 3. Validar schema
    print("[3/4] Validando schema...")
    validate_schema(df, REQUIRED_COLUMNS)
    print("      Schema válido ✓")
    
    # 4. Escribir Parquet en Bronze
    print(f"[4/4] Escribiendo Parquet en s3://{BRONZE_BUCKET}/{BRONZE_KEY}...")
    write_parquet_to_s3(df, BRONZE_BUCKET, BRONZE_KEY)
    print(f"      {len(df)} filas escritas ✓")
    
    print("=" * 60)
    print("JOB STAGING completado exitosamente")
    print("=" * 60)

if __name__ == '__main__':
    main()
import pandas as pd
import boto3
from faker import Faker
import random
from datetime import datetime, timedelta
import io
import time

# ==========================================
# 1. CONFIGURACIÓN
# ==========================================
BUCKET_NAME = "lakehouse-andres-2026-landing"  # bucket landing
S3_PREFIX = "ecommerce_raw/"                    # Carpeta dentro del bucket
TOTAL_ROWS = 5_000_000                          # 5 millones (mínimo del PDF)
CHUNK_SIZE = 500_000                            # Bloques de 500k para no saturar RAM

fake = Faker('es_ES')

# Datos predefinidos para generar datos realistas y rápidos
PAISES = ["Colombia", "México", "España", "Argentina", "Perú", "Chile", "Ecuador"]
PRODUCTOS = [
    ("85123A", "Taza de cerámica blanca"),
    ("22910", "Cuaderno de notas retro"),
    ("84879", "Lámpara LED de escritorio"),
    ("23456", "Mochila impermeable negra"),
    ("99012", "Set de bolígrafos premium"),
    ("11223", "Botella de agua térmica"),
    ("44556", "Agenda planificadora 2026"),
    ("77889", "Soporte ergonómico para laptop"),
    ("33445", "Audífonos inalámbricos"),
    ("66778", "Teclado mecánico RGB")
]

DEPARTAMENTOS = ["Antioquia", "Cundinamarca", "Valle del Cauca", "Atlántico", 
                 "Santander", "Bolívar", "Nariño", "Boyacá", "Tolima", "Caldas"]

print(f" Iniciando generación de {TOTAL_ROWS:,} filas en bloques de {CHUNK_SIZE:,}...")
print(f" Destino: s3://{BUCKET_NAME}/{S3_PREFIX}")

# ==========================================
# 2. FUNCIÓN PARA GENERAR UN BLOQUE
# ==========================================
def generar_chunk(num_filas, offset=0):
    """Genera un DataFrame de Pandas con datos de e-commerce simulados."""
    datos = []
    
    fecha_fin = datetime.now()
    fecha_inicio = fecha_fin - timedelta(days=730)  # 2 años de datos
    rango_segundos = int((fecha_fin - fecha_inicio).total_seconds())
    
    for i in range(num_filas):
        row_id = offset + i
        stock_code, descripcion = random.choice(PRODUCTOS)
        cantidad = random.randint(1, 50)
        precio = round(random.uniform(5.0, 250.0), 2)
        
        # 5% de devoluciones (cantidad negativa)
        if random.random() < 0.05:
            cantidad = -abs(cantidad)
        
        datos.append({
            "invoice_id": f"INV-{random.randint(100000, 999999)}",
            "stock_code": stock_code,
            "description": descripcion,
            "quantity": cantidad,
            "invoice_date": (fecha_inicio + timedelta(seconds=random.randint(0, rango_segundos))).strftime("%Y-%m-%d %H:%M:%S"),
            "price": precio,
            "customer_id": random.randint(1000, 99999),
            "country": random.choice(PAISES),
            "department": random.choice(DEPARTAMENTOS),
            "age": random.randint(18, 75)
        })
    
    return pd.DataFrame(datos)
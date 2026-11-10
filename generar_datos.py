import pandas as pd
import boto3
from faker import Faker
import random
import json
import io
import time
from datetime import datetime, timedelta

# ==========================================
# 1. CONFIGURACIÓN GENERAL
# ==========================================
BUCKET_NAME = "lakehouse-andres-2026-landing"
FAKE = Faker('es_ES')

# Configuración de Ventas (CSV - 5 Millones de filas)
TOTAL_VENTAS = 5_000_000
CHUNK_VENTAS = 500_000
S3_PREFIX_VENTAS = "ecommerce_raw/"  # Ruta limpia para el crawler/staging

# Configuración de Logs (JSON - 500,000 filas) - Se mantiene como bonus de multi-formato
TOTAL_LOGS = 500_000
CHUNK_LOGS = 100_000
S3_PREFIX_LOGS = "logs_raw/"

# Datos predefinidos para velocidad
PAISES = ["Colombia", "México", "España", "Argentina", "Perú", "Chile", "Ecuador"]
PRODUCTOS = [
    ("85123A", "Taza de ceramica blanca"), ("22910", "Cuaderno de notas retro"),
    ("84879", "Lampara LED de escritorio"), ("23456", "Mochila impermeable negra"),
    ("99012", "Set de boligrafos premium"), ("11223", "Botella de agua termica"),
    ("44556", "Agenda planificadora 2026"), ("77889", "Soporte ergonomico para laptop"),
    ("33445", "Audifonos inalambricos"), ("66778", "Teclado mecanico RGB")
]
DEPARTAMENTOS = ["Antioquia", "Cundinamarca", "Valle del Cauca", "Atlantico", "Santander"]

print(f" INICIANDO GENERACIÓN MASIVA DE DATOS...")
print(f" Destino: s3://{BUCKET_NAME}/")

s3_client = boto3.client('s3')

# ==========================================
# 2. FUNCIONES DE GENERACIÓN
# ==========================================
def generar_ventas_chunk(num_filas, offset=0):
    """Genera un DataFrame de Pandas con datos de e-commerce."""
    datos = []
    fecha_fin = datetime.now()
    fecha_inicio = fecha_fin - timedelta(days=730)
    rango_segundos = int((fecha_fin - fecha_inicio).total_seconds())
    
    for i in range(num_filas):
        row_id = offset + i
        stock_code, descripcion = random.choice(PRODUCTOS)
        cantidad = random.randint(1, 50)
        precio = round(random.uniform(5.0, 250.0), 2)
        if random.random() < 0.05: cantidad = -abs(cantidad)
        
        # Sanitización básica para CSV: evitar saltos de línea en la descripción
        descripcion_limpia = descripcion.replace('\n', ' ').replace('\r', ' ')
        
        datos.append({
            "invoice_id": f"INV-{random.randint(100000, 999999)}",
            "stock_code": stock_code,
            "description": descripcion_limpia,
            "quantity": cantidad,
            "invoice_date": (fecha_inicio + timedelta(seconds=random.randint(0, rango_segundos))).strftime("%Y-%m-%d %H:%M:%S"),
            "price": precio,
            "customer_id": random.randint(1000, 99999),
            "country": random.choice(PAISES),
            "department": random.choice(DEPARTAMENTOS),
            "age": random.randint(18, 75)
        })
    return pd.DataFrame(datos)

def generar_logs_chunk(num_filas):
    """Genera una lista de diccionarios (JSON) simulando logs de sistema."""
    logs = []
    fecha_fin = datetime.now()
    fecha_inicio = fecha_fin - timedelta(days=30)
    rango_segundos = int((fecha_fin - fecha_inicio).total_seconds())
    niveles = ["INFO", "WARNING", "ERROR", "DEBUG"]
    eventos = ["login_success", "login_failed", "page_view", "add_to_cart", "checkout_error", "api_timeout"]
    
    for _ in range(num_filas):
        logs.append({
            "log_id": f"LOG-{random.randint(1000000, 9999999)}",
            "timestamp": (fecha_inicio + timedelta(seconds=random.randint(0, rango_segundos))).isoformat(),
            "user_id": random.randint(1000, 99999),
            "event_type": random.choice(eventos),
            "level": random.choice(niveles),
            "message": FAKE.sentence(nb_words=10),
            "ip_address": FAKE.ipv4(),
            "user_agent": FAKE.user_agent()
        })
    return logs

# ==========================================
# 3. EJECUCIÓN: VENTAS (CSV)
# ==========================================
print("\n" + "="*60)
print(" FASE 1: GENERANDO 5 MILLONES DE VENTAS (CSV)")
print("="*60)
start_time = time.time()

for i in range(TOTAL_VENTAS // CHUNK_VENTAS):
    print(f" Ventas: Bloque {i + 1} de {TOTAL_VENTAS // CHUNK_VENTAS}...")
    df = generar_ventas_chunk(CHUNK_VENTAS, offset=i * CHUNK_VENTAS)
    
    # CAMBIO CLAVE: Guardar como CSV en lugar de Parquet
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False, quoting=1) # quoting=1 asegura que las comas dentro de textos no rompan el CSV
    
    s3_key = f"{S3_PREFIX_VENTAS}part_{i:04d}.csv"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue().encode('utf-8'))
    print(f" Ventas bloque {i + 1} subido como CSV.")

# ==========================================
# 4. EJECUCIÓN: LOGS (JSON)
# ==========================================
print("\n" + "="*60)
print(" FASE 2: GENERANDO 500K LOGS DE SISTEMA (JSON)")
print("="*60)

for i in range(TOTAL_LOGS // CHUNK_LOGS):
    print(f" Logs: Bloque {i + 1} de {TOTAL_LOGS // CHUNK_LOGS}...")
    logs = generar_logs_chunk(CHUNK_LOGS)
    json_string = '\n'.join(json.dumps(log) for log in logs)
    
    s3_key = f"{S3_PREFIX_LOGS}logs_part_{i:04d}.json"
    s3_client.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=json_string.encode('utf-8'))
    print(f" Logs bloque {i + 1} subido.")

# ==========================================
# 5. RESUMEN FINAL
# ==========================================
end_time = time.time()
total_time = end_time - start_time

print("\n" + "="*60)
print(" ¡PROCESO COMPLETADO EXITOSAMENTE!")
print(f" Total de ventas generadas: {TOTAL_VENTAS:,} (Formato CSV)")
print(f" Total de logs generados: {TOTAL_LOGS:,} (Formato JSON)")
print(f" Tiempo total de ejecución: {total_time:.2f} segundos")
print(f" Ubicación en S3: s3://{BUCKET_NAME}/")
print("="*60)
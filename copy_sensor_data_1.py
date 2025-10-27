import psycopg2
from datetime import datetime

# === CONFIGURACIÓN DE CONEXIONES ===
ORIGEN = {
    "host": "localhost",
    "database": "HomeBase",
    "user": "postgres",
    "password": "JONPER"
}

DESTINO = {
    "host": "localhost",
    "database": "postgres",
    "user": "postgres",
    "password": "JONPER"
}

TABLA = '"MV_PIEZOMETROS".sensor_data_1'  # esquema.tabla destino
TABLA_ORIGEN = 'public.sensor_data_1'     # esquema.tabla origen


print("⏳ Iniciando copia de datos:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

try:
    # Conexión a base de datos origen
    conn_origen = psycopg2.connect(**ORIGEN)
    cur_origen = conn_origen.cursor()
    cur_origen.execute(f"SELECT * FROM {TABLA_ORIGEN};")
    datos = cur_origen.fetchall()
    columnas = len(cur_origen.description)  # número de columnas detectadas

    # Conexión a base de datos destino
    conn_destino = psycopg2.connect(**DESTINO)
    cur_destino = conn_destino.cursor()

    # Vaciar tabla destino antes de copiar (opcional)
    cur_destino.execute(f"TRUNCATE TABLE {TABLA};")

    # Insertar registros automáticamente según número de columnas
    placeholders = ', '.join(['%s'] * columnas)
    insert_query = f"INSERT INTO {TABLA} VALUES ({placeholders})"

    # Insertar todos los registros
    for fila in datos:
        cur_destino.execute(insert_query, fila)

    conn_destino.commit()

    print(f"✅ Copia completada correctamente ({len(datos)} registros).")

except Exception as e:
    print("❌ Error durante la copia:", e)

finally:
    # Cierre de conexiones
    if 'cur_origen' in locals(): cur_origen.close()
    if 'conn_origen' in locals(): conn_origen.close()
    if 'cur_destino' in locals(): cur_destino.close()
    if 'conn_destino' in locals(): conn_destino.close()

    print("📦 Proceso finalizado:", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

import os
import time
import importlib.util
import psycopg2
import pandas as pd

inicio = time.time() 

# ========= 1. CONFIGURACIÓN DE CONEXIÓN =========
conexion = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="JONPER",
    port="5432"
)
cursor = conexion.cursor()

# ========= 2. LEER DICCIONARIO (preferir diccionario_pz.py, fallback a Excel) =========
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
diccionario_path_py = os.path.join(BASE_DIR, "diccionario_pz.py")
mapping = {}
if os.path.exists(diccionario_path_py):
    try:
        spec = importlib.util.spec_from_file_location("diccionario_pz", diccionario_path_py)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)

        # Soporta dos formatos en diccionario_pz.py:
        #  - DICCIONARIO_SENSORES: {alias: canonical}
        #  - INSTRUMENTOS_VARIANTES: {canonical: [alias1, alias2,...]}
        if hasattr(mod, "DICCIONARIO_SENSORES"):
            for k, v in getattr(mod, "DICCIONARIO_SENSORES").items():
                mapping[str(k).strip().upper()] = str(v).strip().upper()
        elif hasattr(mod, "INSTRUMENTOS_VARIANTES"):
            for canon, variantes in getattr(mod, "INSTRUMENTOS_VARIANTES").items():
                for sc in variantes:
                    mapping[str(sc).strip().upper()] = str(canon).strip().upper()
        else:
            print("⚠️ diccionario_pz.py cargado pero no contiene DICCIONARIO_SENSORES ni INSTRUMENTOS_VARIANTES.")
    except Exception as ex:
        print(f"⚠️ Error cargando diccionario_pz.py: {ex}")

# Si mapping quedó vacío, intentar leer Excel como fallback
if not mapping:
    excel_path = os.path.join(BASE_DIR, "diccionario_pz.xlsx")
    if os.path.exists(excel_path):
        try:
            df_dic = pd.read_excel(excel_path, sheet_name="Diccionario")
            df_dic["sensor_code"] = df_dic["sensor_code"].astype(str).str.strip().str.upper()
            df_dic["id_instrumento"] = df_dic["id_instrumento"].astype(str).str.strip().str.upper()
            for _, r in df_dic.iterrows():
                mapping[r["sensor_code"]] = r["id_instrumento"]
        except Exception as ex:
            print(f"⚠️ No se pudo leer {excel_path}: {ex}")
    else:
        print("⚠️ No se encontró diccionario_pz.py ni diccionario_pz.xlsx. El mapeo quedará vacío.")

# Construir DataFrame a partir del mapping (alias -> canonical)
diccionario = pd.DataFrame([(k, v) for k, v in mapping.items()], columns=["sensor_code", "id_instrumento"])
diccionario["sensor_code"] = diccionario["sensor_code"].astype(str).str.strip().str.upper()
diccionario["id_instrumento"] = diccionario["id_instrumento"].astype(str).str.strip().str.upper()

# Guardar versión plana del diccionario para edición manual
try:
    dic_flat_path = os.path.join(BASE_DIR, "diccionario_flat.csv")
    diccionario.to_csv(dic_flat_path, index=False, encoding="utf-8")
except Exception:
    pass

# ========= ELIMINAR DUPLICADOS DE SENSOR_DATA_1 =========
print("🧹 Eliminando duplicados en sensor_data_1...")
cursor.execute("""
    DELETE FROM "MV_PIEZOMETROS".sensor_data_1 a
    USING "MV_PIEZOMETROS".sensor_data_1 b
    WHERE a.sensor_code = b.sensor_code
      AND a.data_time = b.data_time
      AND a.ctid < b.ctid;
""")
duplicados_borrados = cursor.rowcount  # número de filas eliminadas
conexion.commit()

print(f"✅ Duplicados eliminados: {duplicados_borrados}")


# ========= 3. LEER DATOS DESDE LA TABLA SENSOR_DATA_1 =========
query_sensor = """
SELECT sensor_code,
       CAST(data_time AS TIMESTAMP) AS fecha_hora, 
       CAST(altitude AS NUMERIC) AS nivel_agua_pz,
       CAST(saturation AS NUMERIC) AS columna_agua
FROM "MV_PIEZOMETROS".sensor_data_1;
"""
# Ejecutar con psycopg2 y construir DataFrame manualmente (evita requerir SQLAlchemy)
cursor.execute(query_sensor)
rows = cursor.fetchall()
cols = [desc[0] for desc in cursor.description]
sensor_data = pd.DataFrame(rows, columns=cols)
print(f"📊 Registros leídos desde sensor_data_1: {len(sensor_data)}")

# ========= 4. UNIR LOS DATOS CON EL DICCIONARIO =========
datos_combinados = pd.merge(sensor_data, diccionario, on="sensor_code", how="inner")

# ========= 5. CREAR COLUMNA ID ÚNICA =========
# Usar la columna 'fecha_hora' que se creó en la consulta SQL (alias de data_time)
if "fecha_hora" in datos_combinados.columns:
    datos_combinados["id"] = datos_combinados["fecha_hora"].astype(str) + "_" + datos_combinados["id_instrumento"]
else:
    # Fallback: si existe data_time por alguna razón, usarla; si no, crear id con index
    if "data_time" in datos_combinados.columns:
        datos_combinados["id"] = datos_combinados["data_time"].astype(str) + "_" + datos_combinados["id_instrumento"]
    else:
        datos_combinados["id"] = datos_combinados.index.astype(str) + "_" + datos_combinados["id_instrumento"]

# ========= 6. OBTENER IDS EXISTENTES PARA EVITAR DUPLICADOS (usar schema completo) =========
cursor.execute('SELECT id FROM "MV_PIEZOMETROS"."03_niveles_pz";')
ids_existentes = [row[0] for row in cursor.fetchall()]

# ========= 7. FILTRAR SOLO REGISTROS NUEVOS =========
nuevos_registros = datos_combinados[~datos_combinados["id"].isin(ids_existentes)]

# ========= 8. INSERTAR REGISTROS MANUALMENTE =========
insertados = 0
saltados = 0

for _, fila in nuevos_registros.iterrows():
    try:
        cursor.execute("""
            INSERT INTO "MV_PIEZOMETROS"."03_niveles_pz" (id, id_instrumento, fecha_hora, nivel_agua_pz, columna_agua)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            fila.get("id"),
            fila["id_instrumento"],
            fila["fecha_hora"],
            fila["nivel_agua_pz"],
            fila["columna_agua"]
        ))
        insertados += 1
    except psycopg2.errors.UniqueViolation:
        conexion.rollback()
        saltados += 1
    except Exception as e:
        conexion.rollback()
        print(f"⚠️ Error al insertar registro {fila['id_instrumento']}: {e}")

conexion.commit()

# ========= 9. RESUMEN FINAL =========
print(f"📊 Registros leídos desde sensor_data_1: {len(sensor_data)}")
print(f"✅ Registros nuevos insertados: {insertados}")
print(f"⚠️ Registros duplicados o saltados: {saltados}")
print(f"Total procesados: {len(nuevos_registros)}")

# ========= 10. CERRAR CONEXIÓN =========
cursor.close()
conexion.close()

fin = time.time()  # fin del conteo de tiempo
tiempo_total = fin - inicio
minutos = int(tiempo_total // 60)
segundos = tiempo_total % 60
print(f"⏱ Tiempo total de procesamiento: {minutos} min {segundos:.2f} seg")
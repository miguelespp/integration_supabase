import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv
import numpy as np
import io

# --- 1. CONFIGURACIÓN ---
load_dotenv()

# Variables de conexión PostgreSQL individuales
PGHOST = os.environ.get("PGHOST")
PGPORT = os.environ.get("PGPORT")
PGUSER = os.environ.get("PGUSER")
PGPASSWORD = os.environ.get("PGPASSWORD")
PGDATABASE = os.environ.get("PGDATABASE")

# Validar que todas las variables de PostgreSQL estén presentes
required_vars = {"PGHOST": PGHOST, "PGPORT": PGPORT, "PGUSER": PGUSER, "PGPASSWORD": PGPASSWORD, "PGDATABASE": PGDATABASE}
missing_vars = [var for var, value in required_vars.items() if not value]
if missing_vars:
    print(f"❌ Error: Variables de entorno faltantes: {', '.join(missing_vars)}")
    print("Por favor, configura estas variables en tu archivo .env")
    exit(1)

# AJUSTE: Cambia el nombre del archivo a .xlsx
xlsx_file_path = 'Transporte Terrestre Carga Internacional_2023-2024.xlsx' # ¡Asegúrate de que este sea el nombre correcto!

# --- 2. EXTRACCIÓN Y LIMPIEZA INICIAL ---
print("Iniciando el proceso de ETL...")
try:
    # AJUSTE: Usa pd.read_excel() en lugar de pd.read_csv()
    df = pd.read_excel(xlsx_file_path)
    print(f"✅ Archivo XLSX '{xlsx_file_path}' cargado con {len(df)} filas.")

    # (El resto de la limpieza es idéntica)
    df.rename(columns={
        'RAZON_SOCIAL': 'RAZON_SOCIAL', 'FECHA RESOLUCION': 'FECHA_RESOLUCION',
        'VIGENCIA HASTA': 'VIGENCIA_HASTA', 'MOTIVO_HABILITAC': 'PERMISO_OPER',
        'TIPO_SERVICIO': 'SERVICIO', 'N_EJES': 'N_EJES', 'ANIO_FAB': 'ANIO_FAB',
        'N_MOTOR': 'N_MOTOR', 'CARGA_UTIL': 'CARGA_UTIL', 'P_SECO': 'P_SECO',
        'P_BRUTO': 'P_BRUTO', 'N_ASIENTOS': 'N_ASIENTOS', 'FECHA DE CORTE': 'FECHA_CORTE'
    }, inplace=True)

    df['VIGENCIA_HASTA'] = df['VIGENCIA_HASTA'].replace(88881231, np.nan)
    for col in ['FECHA_RESOLUCION', 'VIGENCIA_HASTA', 'FECHA_CORTE']:
        # Al leer de Excel, las fechas pueden ser interpretadas como timestamps
        # nos aseguramos de convertirlas a string en el formato correcto
        df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')

    print("✅ Renombrado de columnas y limpieza de datos completado.")
    
    # --- NUEVA SECCIÓN: CONVERSIÓN DE TIPOS DE DATO ---
    # Lista de columnas que deben ser enteros
    integer_columns = ['ANIO_FAB', 'N_EJES', 'N_ASIENTOS', 'N_LLANTAS']
    for col in integer_columns:
        if col in df.columns:
            # Usamos 'Int64' que soporta valores nulos (NaN)
            df[col] = df[col].astype('Int64')
    print("✅ Tipos de datos numéricos convertidos a enteros.")

except FileNotFoundError:
    print(f"❌ Error: El archivo '{xlsx_file_path}' no fue encontrado.")
    print("Verifica que el archivo exista en el directorio actual.")
    exit(1)
except pd.errors.EmptyDataError:
    print(f"❌ Error: El archivo '{xlsx_file_path}' está vacío.")
    exit(1)
except Exception as e:
    print(f"❌ Error al cargar o limpiar el XLSX: {e}")
    print("Verifica que el archivo sea un Excel válido y tenga las columnas esperadas.")
    exit(1)

# --- 3. TRANSFORMACIÓN: CREAR TABLAS DE DIMENSIÓN ---
print("\nTransformando datos para las tablas de dimensión...")
try:
    # Validar que las columnas esperadas existen
    required_columns = ['RUC', 'RAZON_SOCIAL', 'PLACA', 'ANIO_FAB', 'N_CHASIS', 'N_MOTOR', 
                       'MARCA', 'SERVICIO', 'CLASE', 'COMBUSTIBLE', 'FECHA_RESOLUCION', 
                       'VIGENCIA_HASTA', 'FECHA_CORTE', 'N_ASIENTOS', 'N_LLANTAS', 'CARGA_UTIL', 
                       'P_SECO', 'P_BRUTO', 'LARGO', 'ANCHO', 'ALTO']
    
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        print(f"⚠️  Advertencia: Columnas faltantes en el archivo: {missing_columns}")
        print("El proceso continuará pero algunas tablas pueden tener problemas.")
    
    dim_empresa = df[['RUC', 'RAZON_SOCIAL']].drop_duplicates().reset_index(drop=True)
    dim_empresa['ID_EMPRESA'] = dim_empresa.index + 1

    cols_caracteristicas = ['N_ASIENTOS', 'N_LLANTAS', 'CARGA_UTIL', 'P_SECO', 'P_BRUTO', 'LARGO', 'ANCHO', 'ALTO']
    # Filtrar solo las columnas que existen
    cols_caracteristicas = [col for col in cols_caracteristicas if col in df.columns]
    dim_caracteristicas = df[cols_caracteristicas].drop_duplicates().reset_index(drop=True)
    dim_caracteristicas['ID_CARACTERISTICAS_TECNICAS'] = dim_caracteristicas.index + 1

    dim_vehiculo = df[['PLACA', 'ANIO_FAB', 'N_CHASIS', 'N_MOTOR', 'MARCA', 'SERVICIO', 'CLASE']].drop_duplicates(subset=['PLACA']).reset_index(drop=True)
    dim_vehiculo['ID_VEHICULO'] = dim_vehiculo.index + 1

    dim_combustible = df[['COMBUSTIBLE']].drop_duplicates().dropna().reset_index(drop=True)
    dim_combustible['ID_COMBUSTIBLE'] = dim_combustible.index + 1

    dim_tiempo = df[['FECHA_RESOLUCION', 'VIGENCIA_HASTA', 'FECHA_CORTE']].drop_duplicates().reset_index(drop=True)
    dim_tiempo['ANIO'] = pd.to_datetime(dim_tiempo['FECHA_CORTE'], errors='coerce').dt.year
    dim_tiempo['MES'] = pd.to_datetime(dim_tiempo['FECHA_CORTE'], errors='coerce').dt.month
    dim_tiempo['ID_TIEMPO'] = dim_tiempo.index + 1
    print("✅ Tablas de dimensión creadas en memoria.")

except KeyError as e:
    print(f"❌ Error: Columna requerida no encontrada: {e}")
    print("Verifica que el archivo tenga todas las columnas esperadas.")
    exit(1)
except Exception as e:
    print(f"❌ Error al crear tablas de dimensión: {e}")
    exit(1)


# --- 4. PREPARACIÓN DE LA TABLA DE HECHOS ---
print("\nPreparando la tabla de hechos (HECHO_VEHICULO_CARGA)...")
try:
    hecho_vehiculo_carga = df.copy()
    hecho_vehiculo_carga = pd.merge(hecho_vehiculo_carga, dim_empresa, on=['RUC', 'RAZON_SOCIAL'], how='left')
    hecho_vehiculo_carga = pd.merge(hecho_vehiculo_carga, dim_vehiculo, on='PLACA', suffixes=('', '_y'), how='left')
    hecho_vehiculo_carga = pd.merge(hecho_vehiculo_carga, dim_caracteristicas, on=cols_caracteristicas, how='left')
    hecho_vehiculo_carga = pd.merge(hecho_vehiculo_carga, dim_combustible, on='COMBUSTIBLE', how='left')
    hecho_vehiculo_carga = pd.merge(hecho_vehiculo_carga, dim_tiempo, on=['FECHA_RESOLUCION', 'VIGENCIA_HASTA', 'FECHA_CORTE'], how='left')

    id_columns = ['ID_TIEMPO', 'ID_EMPRESA', 'ID_VEHICULO', 'ID_CARACTERISTICAS_TECNICAS', 'ID_COMBUSTIBLE']
    
    for col in id_columns:
        if col in hecho_vehiculo_carga.columns:
            hecho_vehiculo_carga[col] = hecho_vehiculo_carga[col].astype('Int64')

    # 1. Seleccionar las columnas finales
    hecho_vehiculo_carga = hecho_vehiculo_carga[id_columns + ['PERMISO_OPER']]
    
    # 2. Re-insertar el bloque de verificación de nulos
    null_counts = hecho_vehiculo_carga.isnull().sum()
    if null_counts.any():
        print("⚠️  Advertencia: Se encontraron valores nulos en las claves foráneas:")
        for col, count in null_counts[null_counts > 0].items():
            print(f"   - {col}: {count} valores nulos")
    
    print("✅ Tabla de hechos preparada.")
    
except Exception as e:
    print(f"❌ Error al preparar tabla de hechos: {e}")
    exit(1)

# --- 5. CARGA DE DATOS A POSTGRESQL CON PSYCOPG2 ---
# (Esta sección no cambia en absoluto)
def upload_df_to_postgres(conn, df, table_name):
    """Carga un DataFrame a PostgreSQL con manejo robusto de errores."""
    print(f"\nIniciando carga a la tabla '{table_name}'...")
    
    if df.empty:
        print(f"⚠️  Advertencia: DataFrame vacío para '{table_name}', omitiendo carga.")
        return False
    
    buffer = io.StringIO()
    df.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
    buffer.seek(0)
    
    cursor = None
    try:
        cursor = conn.cursor()
        columns = ','.join(list(df.columns))
        sql_query = f"COPY {table_name} ({columns}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
        cursor.copy_expert(sql=sql_query, file=buffer)
        conn.commit()
        print(f"✅ Carga de {len(df)} registros a '{table_name}' completada.")
        return True
    except psycopg2.Error as error:
        print(f"❌ Error de PostgreSQL al cargar '{table_name}': {error}")
        conn.rollback()
        return False
    except Exception as error:
        print(f"❌ Error inesperado al cargar '{table_name}': {error}")
        conn.rollback()
        return False
    finally:
        if cursor:
            cursor.close()

conn = None
try:
    conn = psycopg2.connect(
        host=PGHOST,
        port=PGPORT,
        user=PGUSER,
        password=PGPASSWORD,
        database=PGDATABASE
    )
    print("\n✅ Conexión a la base de datos PostgreSQL exitosa.")
    
    # --- AÑADIMOS EL REORDENAMIENTO DE COLUMNAS AQUÍ ---
    
    # 1. Reordenar DIM_EMPRESA
    dim_empresa = dim_empresa[['ID_EMPRESA', 'RUC', 'RAZON_SOCIAL']]
    
    # 2. Reordenar DIM_VEHICULO y eliminar columnas duplicadas si existen
    cols_vehiculo = ['ID_VEHICULO', 'PLACA', 'ANIO_FAB', 'N_CHASIS', 'N_MOTOR', 'MARCA', 'SERVICIO', 'CLASE']
    dim_vehiculo = dim_vehiculo[[col for col in cols_vehiculo if col in dim_vehiculo.columns]]
    
    # 3. Reordenar DIM_CARACTERISTICAS_TECNICAS
    cols_caracteristicas.insert(0, 'ID_CARACTERISTICAS_TECNICAS')
    dim_caracteristicas = dim_caracteristicas[[col for col in cols_caracteristicas if col in dim_caracteristicas.columns]]

    # 4. Reordenar DIM_COMBUSTIBLE
    dim_combustible = dim_combustible[['ID_COMBUSTIBLE', 'COMBUSTIBLE']]
    
    # 5. Reordenar DIM_TIEMPO
    dim_tiempo = dim_tiempo[['ID_TIEMPO', 'FECHA_RESOLUCION', 'VIGENCIA_HASTA', 'FECHA_CORTE', 'ANIO', 'MES']]
    
    # 6. HECHO_VEHICULO_CARGA ya debería estar en el orden correcto, pero lo forzamos para asegurar
    hecho_vehiculo_carga = hecho_vehiculo_carga[['ID_TIEMPO', 'ID_EMPRESA', 'ID_VEHICULO', 'ID_CARACTERISTICAS_TECNICAS', 'ID_COMBUSTIBLE', 'PERMISO_OPER']]

    # Lista de tablas a cargar (ya reordenadas)
    tables_to_load = [
        (dim_empresa, 'DIM_EMPRESA'),
        (dim_vehiculo, 'DIM_VEHICULO'),
        (dim_caracteristicas, 'DIM_CARACTERISTICAS_TECNICAS'),
        (dim_combustible, 'DIM_COMBUSTIBLE'),
        (dim_tiempo, 'DIM_TIEMPO'),
        (hecho_vehiculo_carga, 'HECHO_VEHICULO_CARGA')
    ]
    
    successful_loads = 0
    failed_loads = 0
    
    for df_table, table_name in tables_to_load:
        if upload_df_to_postgres(conn, df_table, table_name):
            successful_loads += 1
        else:
            failed_loads += 1
    
    print(f"\n📊 Resumen de carga:")
    print(f"   ✅ Tablas cargadas exitosamente: {successful_loads}")
    print(f"   ❌ Tablas con errores: {failed_loads}")
    
    if failed_loads > 0:
        print("⚠️  Se completó con errores. Revisa los mensajes anteriores.")

except psycopg2.OperationalError as error:
    print("❌ Error de conexión con PostgreSQL:")
    print(f"   {error}")
    print("Verifica que PostgreSQL esté ejecutándose y las credenciales sean correctas.")
except psycopg2.Error as error:
    print("❌ Error de PostgreSQL:", error)
except Exception as error:
    print("❌ Error inesperado:", error)
finally:
    if conn is not None:
        conn.close()
        print("\nConexión a la base de datos cerrada.")

print("\n🎉 ¡Proceso ETL completado!")
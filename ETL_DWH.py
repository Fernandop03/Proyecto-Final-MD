import pandas as pd
import numpy as np 
import mysql.connector
import time

# --- Configuración de la Base de Datos (la misma que antes) ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'masterf01', 
    'database': 'DeclaracionesAnualesDB'
}

BATCH_INSERT_SIZE = 10000 
# *** CAMBIO IMPORTANTE AQUÍ: Nombre de la tabla OLTP generalizada ***
NOMBRE_TABLA_OLTP = 'declaraciones_pm_anuales' 

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        print("¡Conexión exitosa a la base de datos MySQL!")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def limpiar_tablas_dw(conn):
    cursor = conn.cursor()
    print("Limpiando tablas del Data Warehouse...")
    try:
        cursor.execute("SET FOREIGN_KEY_CHECKS = 0;")
        cursor.execute("TRUNCATE TABLE fact_declaraciones;")
        cursor.execute("TRUNCATE TABLE dim_contribuyente;")
        cursor.execute("TRUNCATE TABLE dim_tiempo;")
        cursor.execute("TRUNCATE TABLE dim_calidad_dato;")
        cursor.execute("SET FOREIGN_KEY_CHECKS = 1;")
        conn.commit()
        print("Tablas del DW limpiadas exitosamente.")
    except Exception as e:
        print(f"Error al limpiar las tablas del DW: {e}")
        conn.rollback()
    finally:
        cursor.close()

def poblar_dim_tiempo(conn):
    cursor = conn.cursor()
    print("Poblando dim_tiempo...")
    try:
        # *** CAMBIO IMPORTANTE AQUÍ: Usar el nombre de tabla correcto ***
        sql_select = f"SELECT DISTINCT ejercicio FROM {NOMBRE_TABLA_OLTP};"
        cursor.execute(sql_select)
        ejercicios = cursor.fetchall()
        
        sql_insert = "INSERT IGNORE INTO dim_tiempo (ejercicio) VALUES (%s);"
        cursor.executemany(sql_insert, ejercicios)
        conn.commit()
        print(f"dim_tiempo poblada con {cursor.rowcount} nuevo(s) registro(s).")
    except Exception as e:
        print(f"Error al poblar dim_tiempo: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def poblar_dim_contribuyente(conn):
    cursor = conn.cursor()
    print("Poblando dim_contribuyente...")
    try:
        # *** CAMBIO IMPORTANTE AQUÍ: Usar el nombre de tabla correcto ***
        sql_insert_select = f"""
        INSERT INTO dim_contribuyente (rfc_anon)
        SELECT DISTINCT rfc_anon FROM {NOMBRE_TABLA_OLTP}; 
        """
        cursor.execute(sql_insert_select)
        conn.commit()
        print(f"dim_contribuyente poblada con {cursor.rowcount} registros.")
    except Exception as e:
        print(f"Error al poblar dim_contribuyente: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def poblar_dim_calidad_dato(conn):
    cursor = conn.cursor()
    print("Poblando dim_calidad_dato...")
    try:
        categorias = [
            ('Valido', 'NINGUNO'),
            ('Invalido', 'INCONSISTENCIA_UPAPTU'),
            ('Invalido', 'INCONSISTENCIA_TOTES'),
            ('Invalido', 'OTRO') 
        ]
        sql_insert = "INSERT INTO dim_calidad_dato (estado_validacion, codigo_error_principal) VALUES (%s, %s);"
        cursor.executemany(sql_insert, categorias)
        conn.commit()
        print(f"dim_calidad_dato poblada con {cursor.rowcount} categorías.")
    except Exception as e:
        print(f"Error al poblar dim_calidad_dato: {e}")
        conn.rollback()
    finally:
        cursor.close()
        
def poblar_fact_declaraciones(conn):
    print("\nIniciando población de fact_declaraciones...")
    
    print("Cargando dimensiones en memoria...")
    tiempo_map = pd.read_sql("SELECT tiempo_key, ejercicio FROM dim_tiempo", conn).set_index('ejercicio')['tiempo_key']
    contrib_map = pd.read_sql("SELECT contribuyente_key, rfc_anon FROM dim_contribuyente", conn).set_index('rfc_anon')['contribuyente_key']
    calidad_df = pd.read_sql("SELECT calidad_key, estado_validacion, codigo_error_principal FROM dim_calidad_dato", conn)
    calidad_df['map_key'] = list(zip(calidad_df['estado_validacion'], calidad_df['codigo_error_principal']))
    calidad_map = calidad_df.set_index('map_key')['calidad_key']
    print("Dimensiones cargadas.")

    # *** CAMBIO IMPORTANTE AQUÍ: Usar el nombre de tabla correcto ***
    sql_extract = f"""
    SELECT 
        d.*,
        (SELECT e.codigoerror FROM erroresvalidaciondeclaraciones e 
         WHERE e.id_declaracion_fk = d.id_declaracion 
         ORDER BY e.errorid LIMIT 1) as codigo_error
    FROM 
        {NOMBRE_TABLA_OLTP} d 
    """
    
    metric_columns = [
        'it_aa', 'td_aa', 'upaptu_c_aa', 'ptu_aa', 'ufe_c_aa', 'pfe_c_aa', 
        'pfea_aa', 'dafpe_aa', 'rfis_c_aa', 'isreje_c_aa', 'cfdim_aa', 'rpm_aa', 
        'orisr_aa', 'impceje_c_aa', 'estec_aa', 'escine_aa', 'estea_aa', 
        'otroes_aa', 'totes_c_aa', 'ppcon_aa', 'ppef_aa', 'imrc_aa', 'imape_aa', 
        'imad_aa', 'cfietu_aa', 'imccfc_aa', 'imccfe_aa', 'occ_aa', 'ocf_aa', 
        'difcar_c_aa', 'diffav_c_aa', 'imide_aa', 'impsun_aa', 'isrcareje_c_aa', 
        'isrpeimpac_aa', 'isrfaveje_c_aa'
    ]
    
    total_rows_inserted_in_fact = 0
    start_time = time.time()
    
    print(f"Leyendo todos los datos de {NOMBRE_TABLA_OLTP} para fact_declaraciones...")
    try:
        df_oltp_completo_original = pd.read_sql(sql_extract, conn)
        print(f"Se leyeron {len(df_oltp_completo_original)} filas de la tabla OLTP.")
    except Exception as e_read:
        print(f"Error al leer todos los datos de OLTP: {e_read}")
        return 
    
    if not df_oltp_completo_original.empty:
        df_processed = df_oltp_completo_original.copy()
        df_processed.columns = [col.lower() for col in df_processed.columns]

        if 'rfc_anon' not in df_processed.columns:
            print("ERROR CRÍTICO: La columna 'rfc_anon' NO está en el DataFrame df_processed DESPUÉS de forzar minúsculas.")
            return

        print(f"Procesando el DataFrame completo para la tabla de hechos...")
        
        df_processed['contribuyente_key'] = df_processed['rfc_anon'].map(contrib_map)
        df_processed['tiempo_key'] = df_processed['ejercicio'].map(tiempo_map)
        
        df_processed['codigo_error_principal'] = df_processed['codigo_error'].fillna(
            df_processed['estadovalidacion'].apply(lambda x: 'NINGUNO' if x == 'Valido' else 'OTRO')
        )
        df_processed['calidad_map_key'] = list(zip(df_processed['estadovalidacion'], df_processed['codigo_error_principal']))
        df_processed['calidad_key'] = df_processed['calidad_map_key'].map(calidad_map)
        
        if df_processed['contribuyente_key'].isnull().any():
            print(f"ADVERTENCIA: Se encontraron {df_processed['contribuyente_key'].isnull().sum()} valores nulos en 'contribuyente_key'.")
        if df_processed['tiempo_key'].isnull().any():
            print(f"ADVERTENCIA: Se encontraron {df_processed['tiempo_key'].isnull().sum()} valores nulos en 'tiempo_key'.")
        if df_processed['calidad_key'].isnull().any():
            print(f"ADVERTENCIA: Se encontraron {df_processed['calidad_key'].isnull().sum()} valores nulos en 'calidad_key'.")

        original_rows = len(df_processed)
        df_processed.dropna(subset=['contribuyente_key', 'tiempo_key', 'calidad_key'], inplace=True)
        if len(df_processed) < original_rows:
            print(f"ADVERTENCIA: Se descartaron {original_rows - len(df_processed)} filas debido a claves de dimensión faltantes.")

        if not df_processed.empty:
            columnas_para_df_for_insert = ['contribuyente_key', 'tiempo_key', 'calidad_key'] + metric_columns
            
            columnas_faltantes = [col for col in columnas_para_df_for_insert if col not in df_processed.columns]
            if columnas_faltantes:
                print(f"ERROR CRÍTICO: Las siguientes columnas NO existen en df_processed: {columnas_faltantes}")
                return

            df_for_insert = df_processed[columnas_para_df_for_insert].copy()

            print("\n--- INFO: Preparando tuplas y convirtiendo NaN a None ---")
            temp_records = df_for_insert.to_dict(orient='records')
            list_of_tuples_to_insert = []
            for record in temp_records:
                list_of_tuples_to_insert.append(
                    tuple(None if pd.isna(record[col]) else record[col] for col in df_for_insert.columns)
                )
            
            cursor = conn.cursor(buffered=True)
            
            lista_columnas_sql = df_for_insert.columns.tolist() 
            columnas_sql_string = ', '.join(lista_columnas_sql)
            placeholders_sql_string = ', '.join(['%s'] * len(lista_columnas_sql))

            sql_insert_fact = f"""
            INSERT INTO fact_declaraciones 
            ({columnas_sql_string}) 
            VALUES ({placeholders_sql_string})
            """
            
            total_rows_to_insert = len(list_of_tuples_to_insert)
            
            for i_batch in range(0, total_rows_to_insert, BATCH_INSERT_SIZE):
                batch_to_insert = list_of_tuples_to_insert[i_batch : i_batch + BATCH_INSERT_SIZE]
                if not batch_to_insert:
                    continue
                
                current_batch_size = len(batch_to_insert)
                print(f"Intentando insertar lote de {current_batch_size} filas (total insertadas hasta ahora: {total_rows_inserted_in_fact})...")
                try:
                    cursor.executemany(sql_insert_fact, batch_to_insert)
                    conn.commit()
                    total_rows_inserted_in_fact += current_batch_size
                    print(f"Lote insertado. {total_rows_inserted_in_fact} de {total_rows_to_insert} filas insertadas en total en fact_declaraciones.")
                except mysql.connector.Error as err_insert:
                    print(f"Error durante executemany en fact_declaraciones (lote iniciando en índice {i_batch}): {err_insert}")
                    conn.rollback()
                    print("Deteniendo la inserción de lotes debido a un error.")
                    break 
            cursor.close()
        else:
            print(f"DataFrame vacío después de filtrar claves de dimensión nulas. No se insertaron datos en fact_declaraciones.")
    else:
        print(f"No se leyeron datos de la tabla OLTP ({NOMBRE_TABLA_OLTP}) para poblar fact_declaraciones.")
        
    end_time = time.time()
    print(f"\nProceso de población de fact_declaraciones finalizado en {end_time - start_time:.2f} segundos.")

def run_dw_etl():
    conn = get_db_connection()
    if not conn:
        print("Proceso ETL abortado.")
        return
        
    try:
        limpiar_tablas_dw(conn)
        poblar_dim_tiempo(conn)
        poblar_dim_contribuyente(conn)
        poblar_dim_calidad_dato(conn)
        poblar_fact_declaraciones(conn)
        
        print("\n¡Proceso ETL de OLTP a DW finalizado!")
        
    except Exception as e:
        print(f"Ocurrió un error general en el proceso ETL del DW: {e}")
    finally:
        if conn:
            conn.close()
            print("Conexión a la base de datos cerrada.")

if __name__ == "__main__":
    run_dw_etl()
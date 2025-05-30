import pandas as pd
import mysql.connector
from mysql.connector import errorcode
import datetime
import decimal 
import os # Para obtener solo el nombre del archivo

# --- Configuración de Archivo y Ejercicio ---
# CAMBIA ESTOS VALORES SEGÚN EL ARCHIVO QUE QUIERAS PROCESAR
# Para procesar 2014:
CSV_FILE_TO_PROCESS = 'Anuales_ISR_PM_2014.csv' # Asegúrate que este archivo exista
EJERCICIO_FISCAL_ESPERADO = 2014

# Para procesar 2015 (descomenta estas líneas y comenta las de 2014):
#CSV_FILE_TO_PROCESS = 'Anuales_ISR_PM_2015.csv'
#EJERCICIO_FISCAL_ESPERADO = 2015
# --- Fin de Configuración ---

# --- Configuración de la Base de Datos ---
DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 3306,
    'user': 'root',
    'password': 'masterf01', 
    'database': 'DeclaracionesAnualesDB'
}

# --- Metadatos de Columnas (con 'Variable' en minúsculas) ---
column_metadata = [
    {'Variable': 'rfc_anon', 'Descripción': 'Identificador del RFC', 'tipo_dato_original': 'BIGINT', 'tipo_dato_pandas': 'Int64', 'tipo_dato_sql': 'BIGINT', 'obligatorio': True},
    {'Variable': 'ejercicio', 'Descripción': 'Ejercicio Fiscal Presentado', 'tipo_dato_original': 'INTEGER', 'tipo_dato_pandas': 'Int64', 'tipo_dato_sql': 'INT', 'obligatorio': True, 'valor_esperado': EJERCICIO_FISCAL_ESPERADO},
    {'Variable': 'it_aa', 'Descripción': 'TOTAL DE INGRESOS ACUMULABLES', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'td_aa', 'Descripción': 'TOTAL DE DEDUCCIONES AUTORIZADAS Y DEDUCCIÓN INMEDIATA DE INVERSIONES', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'upaptu_c_aa', 'Descripción': 'UTILIDAD O PERDIDA FISCAL ANTES DE PTU', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)'},
    {'Variable': 'ptu_aa', 'Descripción': 'PTU PAGADA EN EL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'ufe_c_aa', 'Descripción': 'UTILIDAD FISCAL DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)'},
    {'Variable': 'pfe_c_aa', 'Descripción': 'PERDIDA FISCAL DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'pfea_aa', 'Descripción': 'PERDIDAS FISCALES DE EJERCICIOS ANTERIORES QUE SE APLICAN EN EL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'dafpe_aa', 'Descripción': 'DEDUCCIÓN ADICIONAL DEL FOMENTO AL PRIMER EMPLEO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'rfis_c_aa', 'Descripción': 'RESULTADO FISCAL', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)'},
    {'Variable': 'isreje_c_aa', 'Descripción': 'IMPUESTO SOBRE LA RENTA DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'cfdim_aa', 'Descripción': 'REDUCCIONES PARA MAQUILADORAS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'rpm_aa', 'Descripción': 'CREDITO FISCAL POR DEDUCCION INMEDIATA PARA MAQUILADORAS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'orisr_aa', 'Descripción': 'OTRAS REDUCCIONES DEL ISR', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'impceje_c_aa', 'Descripción': 'IMPUESTO CAUSADO EN EL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'estec_aa', 'Descripción': 'ESTIMULO POR PROYECTOS EN INVESTIGACIÓN Y DESARROLLO TECNOLÓGICO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'escine_aa', 'Descripción': 'ESTIMULO POR PROYECTOS DE INVERSIÓN EN LA PRODUCCIÓN CINEMATOGRÁFICA NACIONAL', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'estea_aa', 'Descripción': 'ESTIMULO A PROYECTOS DE INVERSIÓN EN LA PRODUCCIÓN TEATRAL NACIONAL', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'otroes_aa', 'Descripción': 'OTROS ESTÍMULOS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'totes_c_aa', 'Descripción': 'TOTAL DE ESTIMULOS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'ppcon_aa', 'Descripción': 'PAGOS PROVISIONALES EFECTUADOS ENTREGADOS A LA CONTROLADORA', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'ppef_aa', 'Descripción': 'PAGOS PROVISIONIALES EFECTUADOS ENTERADOS A LA FEDERACIÓN', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imrc_aa', 'Descripción': 'IMPUESTO RETENIDO AL CONTRIBUYENTE', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imape_aa', 'Descripción': 'IMPUESTO ACREDITARLE PAGADO EN EL EXTRANJERO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imad_aa', 'Descripción': 'IMPUESTO ACREDITARLE POR D1V1DENDOS O UTILIDADES DISTRIBUIDOS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'cfietu_aa', 'Descripción': 'CRÉDITO FISCAL IETU POR DEDUCCIONES MAYORES A LOS INGRESOS', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imccfc_aa', 'Descripción': 'IMPUESTO CORRESPONDIENTE A LA CONSOLIDACIÓN FISCAL A CARGO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imccfe_aa', 'Descripción': 'IMPUESTO CORRESPONDIENTE A LA CONSOLIDACION FISCAL ENTREGADO (en exceso)', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'occ_aa', 'Descripción': 'OTRAS CANTIDADES A CARGO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'ocf_aa', 'Descripción': 'OTRAS CANTIDADES A FAVOR', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'difcar_c_aa', 'Descripción': 'DIFERENCIA A CARGO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'diffav_c_aa', 'Descripción': 'DIFERENCIA A FAVOR', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'imide_aa', 'Descripción': 'IMPUESTO ACREDITARLE POR DEPÓSITOS EN EFECTIVO DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'impsun_aa', 'Descripción': 'IMPUESTO ALA VENTA DE BIENES Y SERVICIOS SUNTUARIOS ACREDITARLE', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'isrcareje_c_aa', 'Descripción': 'ISR A CARGO DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'isrpeimpac_aa', 'Descripción': 'ISR PAGADO EN EXCESO APLICADO CONTRA EL IMPAC', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True},
    {'Variable': 'isrfaveje_c_aa', 'Descripción': 'ISR A FAVOR DEL EJERCICIO', 'tipo_dato_original': 'FLOAT', 'tipo_dato_pandas': 'float64', 'tipo_dato_sql': 'DECIMAL(20, 4)', 'no_negativo': True}
]

CHUNK_SIZE = 100000

def get_db_connection():
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        return conn
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Error: Usuario o contraseña de MySQL incorrectos.")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print(f"Error: La base de datos '{DB_CONFIG['database']}' no existe.")
        else:
            print(f"Error de conexión a la base de datos: {err}")
        return None

def preprocess_chunk(chunk_df, metadata):
    for col_meta in metadata:
        col_name = col_meta['Variable'] 
        if col_name in chunk_df.columns:
            if col_meta['tipo_dato_original'] == 'BIGINT' or col_meta['tipo_dato_original'] == 'INTEGER':
                chunk_df[col_name] = pd.to_numeric(chunk_df[col_name], errors='coerce')
                if not chunk_df[col_name].isnull().all():
                    is_integer_like = (chunk_df[col_name].dropna() % 1 == 0).all()
                    if is_integer_like:
                         chunk_df[col_name] = chunk_df[col_name].astype(col_meta['tipo_dato_pandas'])
            elif col_meta['tipo_dato_original'] == 'FLOAT':
                chunk_df[col_name] = pd.to_numeric(chunk_df[col_name], errors='coerce').astype(col_meta['tipo_dato_pandas'])
    return chunk_df

def read_csv_in_chunks(file_path, chunk_size_param):
    try:
        if not os.path.exists(file_path):
            print(f"Error: Archivo CSV '{file_path}' no encontrado.")
            return None
        print(f"Archivo CSV '{file_path}' será leído en chunks de {chunk_size_param} filas.")
        return pd.read_csv(file_path, delimiter=',', chunksize=chunk_size_param, low_memory=False)
    except Exception as e:
        print(f"Error al leer el archivo CSV en chunks: {e}")
        return None

def iniciar_log_procesamiento(conn, nombre_archivo_completo):
    cursor = conn.cursor()
    usuario_db = "etl_script"
    try:
        cursor.execute("SELECT USER()")
        usuario_db_result = cursor.fetchone()
        if usuario_db_result:
            usuario_db = usuario_db_result[0]
    except Exception as e:
        print(f"Advertencia: No se pudo obtener el usuario de BD, usando default '{usuario_db}'. Error: {e}")

    sql_insert_log = """
    INSERT INTO procesamientologdeclaraciones 
    (nombrearchivo, fechahorainicio, estadogeneral, usuarioejecucion) 
    VALUES (%s, %s, %s, %s)
    """
    ahora = datetime.datetime.now()
    nombre_archivo_base = os.path.basename(nombre_archivo_completo)
    valores = (nombre_archivo_base, ahora, 'Iniciado', usuario_db)
    
    try:
        cursor.execute(sql_insert_log, valores)
        conn.commit()
        log_id = cursor.lastrowid
        print(f"Log de procesamiento iniciado para '{nombre_archivo_base}'. ID: {log_id}")
        return log_id
    except mysql.connector.Error as err:
        print(f"Error al iniciar log de procesamiento para '{nombre_archivo_base}': {err}")
        conn.rollback()
        return None
    finally:
        cursor.close()

def finalizar_log_procesamiento(conn, log_id, estado, total_arch, proc, validos, invalidos, msg=""):
    if not log_id: return
    cursor = conn.cursor()
    sql_update_log = """
    UPDATE procesamientologdeclaraciones SET 
    fechahorafin = %s, estadogeneral = %s, totalregistrosarchivo = %s, 
    registrosprocesados = %s, registrosvalidoscargados = %s, 
    registrosinvalidos = %s, mensaje = %s
    WHERE logid = %s 
    """
    ahora = datetime.datetime.now()
    valores = (ahora, estado, total_arch, proc, validos, invalidos, msg, log_id)
    try:
        cursor.execute(sql_update_log, valores)
        conn.commit()
        print(f"Log de procesamiento ID {log_id} finalizado. Estado: {estado}")
    except mysql.connector.Error as err:
        print(f"Error al finalizar log de procesamiento ID {log_id}: {err}")
        conn.rollback()
    finally:
        cursor.close()

def registrar_error_validacion(conn, log_id, rfc_ref, ejercicio_ref, campo, valor, codigo, descripcion, id_declaracion_fk=None):
    cursor = conn.cursor()
    sql_insert_error = """
    INSERT INTO erroresvalidaciondeclaraciones
    (id_declaracion_fk, rfc_anon_ref, ejercicio_ref, logid_fk, nombrecampo, valorcampoproblematico, codigoerror, descripcionerror)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
    """
    valor_str = str(valor) if valor is not None else None
    rfc_ref_clean = None if pd.isna(rfc_ref) else rfc_ref
    ejercicio_ref_clean = None if pd.isna(ejercicio_ref) else int(ejercicio_ref) if pd.notna(ejercicio_ref) else None

    valores = (id_declaracion_fk, rfc_ref_clean, ejercicio_ref_clean, log_id, campo, valor_str, codigo, descripcion)
    try:
        cursor.execute(sql_insert_error, valores)
        conn.commit()
    except mysql.connector.Error as err:
        print(f"\nError al registrar error de validación para RFC {rfc_ref_clean}, Campo {campo}: {err}")
        conn.rollback()
    finally:
        cursor.close()

def validar_fila(row_series, metadata_list, log_id, conn):
    errores_en_fila = []
    rfc_anon = row_series.get('rfc_anon') 
    ejercicio = row_series.get('ejercicio')

    for col_meta in metadata_list:
        col_name = col_meta['Variable']
        valor = row_series.get(col_name)

        if col_meta.get('obligatorio') and pd.isna(valor):
            desc = f"El campo '{col_name}' es obligatorio y está vacío."
            errores_en_fila.append({'campo': col_name, 'valor': valor, 'codigo': 'OBLIGATORIO_FALTANTE', 'desc': desc})
            if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio, col_name, valor, 'OBLIGATORIO_FALTANTE', desc)
            continue
        
        if pd.isna(valor):
            continue
        
        valor_esperado = col_meta.get('valor_esperado')
        if valor_esperado is not None:
            try:
                tipo_esperado = type(valor_esperado)
                valor_convertido_para_comparacion = tipo_esperado(valor)
                if valor_convertido_para_comparacion != valor_esperado:
                    desc = f"El campo '{col_name}' tiene un valor '{valor}' pero se esperaba '{valor_esperado}'."
                    errores_en_fila.append({'campo': col_name, 'valor': valor, 'codigo': 'VALOR_INESPERADO', 'desc': desc})
                    if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio, col_name, valor, 'VALOR_INESPERADO', desc)
            except ValueError:
                desc = f"El campo '{col_name}' tiene un valor '{valor}' que no se pudo convertir al tipo esperado para la comparación con '{valor_esperado}'."
                errores_en_fila.append({'campo': col_name, 'valor': valor, 'codigo': 'TIPO_DATO_INESPERADO', 'desc': desc})
                if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio, col_name, valor, 'TIPO_DATO_INESPERADO', desc)

        if col_meta.get('no_negativo') and isinstance(valor, (int, float, decimal.Decimal)) and valor < 0:
            desc = f"El campo '{col_name}' tiene un valor negativo '{valor}' y no está permitido."
            errores_en_fila.append({'campo': col_name, 'valor': valor, 'codigo': 'NEGATIVO_NO_PERMITIDO', 'desc': desc})
            if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio, col_name, valor, 'NEGATIVO_NO_PERMITIDO', desc)
        
        if col_name == 'upaptu_c_aa': 
            it_aa_val = row_series.get('it_aa')
            td_aa_val = row_series.get('td_aa')
            if pd.notna(it_aa_val) and pd.notna(td_aa_val) and pd.notna(valor):
                try:
                    calculo_upaptu = decimal.Decimal(str(it_aa_val)) - decimal.Decimal(str(td_aa_val))
                    diferencia = abs(calculo_upaptu - decimal.Decimal(str(valor)))
                    if diferencia > decimal.Decimal('0.01'): 
                        desc_cruzada = (f"Inconsistencia UPAPTU: it_aa ({it_aa_val}) - td_aa ({td_aa_val}) = {calculo_upaptu}, "
                                        f"pero upaptu_c_aa reportado es {valor}. Diferencia: {diferencia:.4f}")
                        errores_en_fila.append({'campo': 'UPAPTU_C_AA_CROSSCHECK', 'valor': valor, 
                                                'codigo': 'INCONSISTENCIA_UPAPTU', 'desc': desc_cruzada})
                        if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio, 
                                                   'UPAPTU_C_AA_CROSSCHECK', f"IT_AA:{it_aa_val},TD_AA:{td_aa_val},UPAPTU_C_AA:{valor}",
                                                   'INCONSISTENCIA_UPAPTU', desc_cruzada)
                except decimal.InvalidOperation: pass 
        
        if col_name == 'totes_c_aa':
            estec_aa_val = row_series.get('estec_aa')
            escine_aa_val = row_series.get('escine_aa')
            estea_aa_val = row_series.get('estea_aa')
            otroes_aa_val = row_series.get('otroes_aa')
            if pd.notna(valor):
                try:
                    suma_estimulos = (decimal.Decimal(str(estec_aa_val)) if pd.notna(estec_aa_val) else decimal.Decimal(0)) + \
                                     (decimal.Decimal(str(escine_aa_val)) if pd.notna(escine_aa_val) else decimal.Decimal(0)) + \
                                     (decimal.Decimal(str(estea_aa_val)) if pd.notna(estea_aa_val) else decimal.Decimal(0)) + \
                                     (decimal.Decimal(str(otroes_aa_val)) if pd.notna(otroes_aa_val) else decimal.Decimal(0))
                    diferencia_totes = abs(suma_estimulos - decimal.Decimal(str(valor)))
                    if diferencia_totes > decimal.Decimal('0.01'):
                        desc_cruzada = (f"Inconsistencia TOTES: Suma de estímulos individuales ({suma_estimulos}) "
                                        f"no coincide con totes_c_aa ({valor}). Diferencia: {diferencia_totes:.4f}")
                        errores_en_fila.append({'campo': 'TOTES_C_AA_CROSSCHECK', 'valor': valor,
                                                'codigo': 'INCONSISTENCIA_TOTES', 'desc': desc_cruzada})
                        if conn: registrar_error_validacion(conn, log_id, rfc_anon, ejercicio,
                                                   'TOTES_C_AA_CROSSCHECK', f"SumaInd:{suma_estimulos},TOTES_C_AA:{valor}",
                                                   'INCONSISTENCIA_TOTES', desc_cruzada)
                except decimal.InvalidOperation: pass
    return errores_en_fila

def insertar_declaracion(conn, data_dict, estado_validacion, metadata_list):
    cursor = conn.cursor()
    final_insert_data = {}
    
    # Solo incluir columnas que existen en el metadata
    columnas_metadata = {meta['Variable'] for meta in metadata_list}
    
    for col_name in data_dict.keys():
        if col_name in columnas_metadata:
            final_insert_data[col_name] = data_dict.get(col_name)

    final_insert_data['estadovalidacion'] = estado_validacion
    
    for key, value in final_insert_data.items():
        if pd.isna(value):
            final_insert_data[key] = None

    sql_columns = ", ".join(final_insert_data.keys())
    sql_placeholders = ", ".join(["%s"] * len(final_insert_data))
    sql_insert = f"INSERT INTO declaraciones_pm_anuales ({sql_columns}) VALUES ({sql_placeholders})"
    
    valores = tuple(final_insert_data.values())

    try:
        cursor.execute(sql_insert, valores)
        conn.commit()
        id_declaracion = cursor.lastrowid
        return id_declaracion, None
    except mysql.connector.Error as err:
        rfc_val = data_dict.get('rfc_anon', 'Desconocido')
        ejercicio_val = data_dict.get('ejercicio', 'Desconocido')
        if err.errno == errorcode.ER_DUP_ENTRY:
             print(f"\nAdvertencia: Registro duplicado para RFC {rfc_val} Ejercicio {ejercicio_val}. No se insertó.")
        else:
            print(f"\nError al insertar declaración para RFC {rfc_val} Ejercicio {ejercicio_val}: {err}")
        conn.rollback()
        return None, err.errno
    finally:
        cursor.close()

# --- NUEVA FUNCIÓN DE LIMPIEZA ---
def limpiar_columnas_inutiles(df, chunk_num):
    """
    Identifica y elimina columnas que son 100% nulas o que solo contienen ceros.
    """
    columnas_a_eliminar = []
    
    # Criterio 1: Columnas 100% vacías (NaN)
    cols_vacias = df.columns[df.isnull().all()].tolist()
    if cols_vacias:
        columnas_a_eliminar.extend(cols_vacias)

    # Criterio 2: Columnas que solo contienen ceros (y posiblemente NaNs)
    # No queremos eliminar columnas clave como 'rfc_anon' o 'ejercicio'
    columnas_clave = ['rfc_anon', 'ejercicio']
    for col in df.columns:
        if col not in columnas_a_eliminar and col not in columnas_clave:
            # Rellenamos los NaN con 0 para la comprobación y vemos si todos los valores son 0
            if pd.to_numeric(df[col], errors='coerce').fillna(0).eq(0).all():
                columnas_a_eliminar.append(col)

    # Eliminar duplicados de la lista y luego las columnas del DataFrame
    columnas_a_eliminar = sorted(list(set(columnas_a_eliminar)))

    if columnas_a_eliminar:
        print(f"\n[Chunk {chunk_num}] Limpieza: Eliminando {len(columnas_a_eliminar)} columnas inútiles (vacías o solo ceros): {columnas_a_eliminar}")
        df_limpio = df.drop(columns=columnas_a_eliminar)
        return df_limpio
    else:
        print(f"\n[Chunk {chunk_num}] Limpieza: No se encontraron columnas inútiles para eliminar.")
        return df

def etl_proceso_declaraciones(path_del_csv_actual, metadata_actual):
    conn = get_db_connection()
    if not conn:
        print(f"Proceso ETL para {path_del_csv_actual} abortado: fallo de conexión a la BD.")
        return

    log_id = iniciar_log_procesamiento(conn, path_del_csv_actual)
    if not log_id:
        print(f"Proceso ETL para {path_del_csv_actual} abortado: fallo al iniciar log.")
        if conn: conn.close()
        return
        
    df_iterator = read_csv_in_chunks(path_del_csv_actual, CHUNK_SIZE)

    if df_iterator is None:
        finalizar_log_procesamiento(conn, log_id, 'Fallido', 0,0,0,0, f"Error crítico al leer/preprocesar {path_del_csv_actual}.")
        if conn: conn.close()
        return

    registros_procesados_global = 0
    registros_validos_cargados_global = 0
    registros_invalidos_global = 0
    total_registros_archivo_acumulado = 0

    print(f"\nIniciando procesamiento de chunks del archivo {path_del_csv_actual}...")

    for i, chunk_df_original in enumerate(df_iterator):
        chunk_num = i + 1
        print(f"\n--- Procesando chunk {chunk_num} de {path_del_csv_actual} ---")
        
        chunk_df = chunk_df_original.copy()
        chunk_df.columns = [col.lower() for col in chunk_df.columns] # Asegurar minúsculas
        
        if chunk_df.empty:
            print("Chunk vacío, omitiendo.")
            continue

        # --- MODIFICACIÓN: LLAMAR A LA FUNCIÓN DE LIMPIEZA AQUÍ ---
        chunk_df = limpiar_columnas_inutiles(chunk_df, chunk_num)

        chunk_df = preprocess_chunk(chunk_df, metadata_actual)
        
        total_registros_chunk = len(chunk_df)
        total_registros_archivo_acumulado += total_registros_chunk
        print(f"Chunk {chunk_num} tiene {total_registros_chunk} filas para procesar después de la limpieza.")

        for index, row in chunk_df.iterrows():
            registros_procesados_global += 1
            if registros_procesados_global % (max(1, CHUNK_SIZE // 20)) == 0 or total_registros_chunk < (max(1,CHUNK_SIZE // 20)) :
                 print(f"\rProcesando fila global {registros_procesados_global} (de archivo {os.path.basename(path_del_csv_actual)})...", end="")
            
            fila_dict = row.to_dict()
            errores = validar_fila(row, metadata_actual, log_id, conn)

            id_declaracion_insertada = None
            insert_errno = None

            if not errores:
                id_declaracion_insertada, insert_errno = insertar_declaracion(conn, fila_dict, 'Valido', metadata_actual)
                if id_declaracion_insertada:
                    registros_validos_cargados_global += 1
                else: 
                    registros_invalidos_global += 1
                    if conn and insert_errno and insert_errno != errorcode.ER_DUP_ENTRY:
                        registrar_error_validacion(conn, log_id, fila_dict.get('rfc_anon'), fila_dict.get('ejercicio'),
                                               "GENERAL_INSERT", str(fila_dict), "ERROR_INSERCION_BD", f"Fallo al insertar registro validado (errno: {insert_errno}).")
            else: 
                registros_invalidos_global += 1
                id_declaracion_insertada, insert_errno_invalid = insertar_declaracion(conn, fila_dict, 'Invalido', metadata_actual)
                if id_declaracion_insertada and conn: 
                    cursor_update = conn.cursor()
                    try:
                        rfc_ref_update = fila_dict.get('rfc_anon')
                        ejercicio_ref_update = fila_dict.get('ejercicio')
                        
                        if pd.notna(rfc_ref_update) and pd.notna(ejercicio_ref_update): # Comprobar NaNs
                            sql_update_errores = """
                            UPDATE erroresvalidaciondeclaraciones 
                            SET id_declaracion_fk = %s 
                            WHERE logid_fk = %s AND rfc_anon_ref = %s AND ejercicio_ref = %s AND id_declaracion_fk IS NULL
                            """
                            # Asegurarse que el ejercicio_ref sea int
                            cursor_update.execute(sql_update_errores, (id_declaracion_insertada, log_id, rfc_ref_update, int(ejercicio_ref_update)))
                            conn.commit()
                    except mysql.connector.Error as err_update:
                        print(f"\nError al actualizar FK en ErroresValidacionDeclaraciones para ID {id_declaracion_insertada}: {err_update}")
                        conn.rollback()
                    finally:
                        cursor_update.close()
                elif conn and insert_errno_invalid and insert_errno_invalid != errorcode.ER_DUP_ENTRY:
                    registrar_error_validacion(conn, log_id, fila_dict.get('rfc_anon'), fila_dict.get('ejercicio'),
                                               "GENERAL_INSERT_INVALID", str(fila_dict), "ERROR_INSERCION_BD_INVALIDO", f"Fallo al insertar registro inválido (errno: {insert_errno_invalid}).")
        
        print(f"\nFin del procesamiento del chunk {chunk_num} de {path_del_csv_actual}.")

    print(f"\nProcesamiento de todos los chunks para {path_del_csv_actual} completado. Total filas leídas: {total_registros_archivo_acumulado}")

    mensaje_final = (f"Archivo: {os.path.basename(path_del_csv_actual)}. Total Filas: {total_registros_archivo_acumulado}, "
                     f"Procesadas: {registros_procesados_global}, Válidas Cargadas: {registros_validos_cargados_global}, "
                     f"Inválidas/No cargadas: {registros_invalidos_global}.")
    
    estado_final = 'Completado'
    if registros_invalidos_global > 0:
        estado_final = 'CompletadoConErrores'
    if registros_procesados_global == 0 and total_registros_archivo_acumulado > 0:
        estado_final = 'Fallido'
    
    finalizar_log_procesamiento(conn, log_id, estado_final, 
                               total_registros_archivo_acumulado, registros_procesados_global, 
                               registros_validos_cargados_global, registros_invalidos_global, 
                               mensaje_final)

    if conn:
        conn.close()
        print(f"Conexión a la base de datos cerrada para el archivo {os.path.basename(path_del_csv_actual)}.")

if __name__ == "__main__":
    print(f"--- INICIO DEL PROCESO ETL PARA {CSV_FILE_TO_PROCESS} (Ejercicio: {EJERCICIO_FISCAL_ESPERADO}) ---")
    etl_proceso_declaraciones(CSV_FILE_TO_PROCESS, column_metadata) # Pasar column_metadata
    print(f"--- FIN DEL PROCESO ETL PARA {CSV_FILE_TO_PROCESS} ---")
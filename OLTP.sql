-- =============================================
-- FASE 0: CREACIÓN DE LA BASE DE DATOS
-- =============================================
CREATE DATABASE IF NOT EXISTS DeclaracionesAnualesDB;
USE DeclaracionesAnualesDB;

-- =============================================
-- FASE 1: ESTRUCTURA OLTP 
-- (Tabla para almacenar datos de CSV, logs y errores)
-- =============================================

-- Tabla principal OLTP para las Declaraciones Anuales de Personas Morales
CREATE TABLE IF NOT EXISTS declaraciones_pm_anuales (
    id_declaracion INT AUTO_INCREMENT PRIMARY KEY,
    rfc_anon BIGINT NOT NULL,
    ejercicio INT NOT NULL,
    it_aa DECIMAL(20, 4) NULL,
    td_aa DECIMAL(20, 4) NULL,
    upaptu_c_aa DECIMAL(20, 4) NULL,
    ptu_aa DECIMAL(20, 4) NULL,
    ufe_c_aa DECIMAL(20, 4) NULL,
    pfe_c_aa DECIMAL(20, 4) NULL,
    pfea_aa DECIMAL(20, 4) NULL,
    dafpe_aa DECIMAL(20, 4) NULL,
    rfis_c_aa DECIMAL(20, 4) NULL,
    isreje_c_aa DECIMAL(20, 4) NULL,
    cfdim_aa DECIMAL(20, 4) NULL,
    rpm_aa DECIMAL(20, 4) NULL,
    orisr_aa DECIMAL(20, 4) NULL,
    impceje_c_aa DECIMAL(20, 4) NULL,
    estec_aa DECIMAL(20, 4) NULL,
    escine_aa DECIMAL(20, 4) NULL,
    estea_aa DECIMAL(20, 4) NULL,
    otroes_aa DECIMAL(20, 4) NULL,
    totes_c_aa DECIMAL(20, 4) NULL,
    ppcon_aa DECIMAL(20, 4) NULL,
    ppef_aa DECIMAL(20, 4) NULL,
    imrc_aa DECIMAL(20, 4) NULL,
    imape_aa DECIMAL(20, 4) NULL,
    imad_aa DECIMAL(20, 4) NULL,
    cfietu_aa DECIMAL(20, 4) NULL,
    imccfc_aa DECIMAL(20, 4) NULL,
    imccfe_aa DECIMAL(20, 4) NULL,
    occ_aa DECIMAL(20, 4) NULL,
    ocf_aa DECIMAL(20, 4) NULL,
    difcar_c_aa DECIMAL(20, 4) NULL,
    diffav_c_aa DECIMAL(20, 4) NULL,
    imide_aa DECIMAL(20, 4) NULL,
    impsun_aa DECIMAL(20, 4) NULL,
    isrcareje_c_aa DECIMAL(20, 4) NULL,
    isrpeimpac_aa DECIMAL(20, 4) NULL,
    isrfaveje_c_aa DECIMAL(20, 4) NULL,
    fechacarga DATETIME DEFAULT CURRENT_TIMESTAMP,
    estadovalidacion VARCHAR(50) DEFAULT 'Pendiente',
    CONSTRAINT uq_rfc_ejercicio UNIQUE (rfc_anon, ejercicio)
);

-- Tabla para el log de procesamiento de archivos (ETL CSV -> OLTP)
CREATE TABLE IF NOT EXISTS procesamientologdeclaraciones (
    logid INT AUTO_INCREMENT PRIMARY KEY,
    nombrearchivo VARCHAR(255),
    fechahorainicio DATETIME,
    fechahorafin DATETIME NULL,
    estadogeneral VARCHAR(50),
    totalregistrosarchivo INT NULL,
    registrosprocesados INT DEFAULT 0,
    registrosvalidoscargados INT DEFAULT 0,
    registrosinvalidos INT DEFAULT 0,
    mensaje TEXT NULL,
    usuarioejecucion VARCHAR(100) NULL
);

-- Tabla para registrar errores de validación (ETL CSV -> OLTP)
CREATE TABLE IF NOT EXISTS erroresvalidaciondeclaraciones (
    errorid INT AUTO_INCREMENT PRIMARY KEY,
    id_declaracion_fk INT NULL, 
    rfc_anon_ref BIGINT NULL, 
    ejercicio_ref INT NULL, 
    logid_fk INT,
    nombrecampo VARCHAR(100),
    valorcampoproblematico TEXT,
    codigoerror VARCHAR(50),
    descripcionerror TEXT,
    fechahoraerror DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (id_declaracion_fk) REFERENCES declaraciones_pm_anuales(id_declaracion) ON DELETE CASCADE,
    FOREIGN KEY (logid_fk) REFERENCES procesamientologdeclaraciones(logid) ON DELETE CASCADE
);

-- =============================================
-- FASE 2: ESTRUCTURA DEL DATA WAREHOUSE (Para ETL OLTP -> DW)
-- =============================================

-- DIMENSIÓN 1: Contribuyente
CREATE TABLE IF NOT EXISTS dim_contribuyente (
    contribuyente_key INT AUTO_INCREMENT PRIMARY KEY,
    rfc_anon BIGINT NOT NULL,
    CONSTRAINT uq_dim_rfc_anon UNIQUE (rfc_anon)
);

-- DIMENSIÓN 2: Tiempo
CREATE TABLE IF NOT EXISTS dim_tiempo (
    tiempo_key INT AUTO_INCREMENT PRIMARY KEY,
    ejercicio INT NOT NULL,
    CONSTRAINT uq_dim_ejercicio UNIQUE (ejercicio)
);

-- DIMENSIÓN 3: Calidad del Dato
CREATE TABLE IF NOT EXISTS dim_calidad_dato (
    calidad_key INT AUTO_INCREMENT PRIMARY KEY,
    estado_validacion VARCHAR(50) NOT NULL,
    codigo_error_principal VARCHAR(50) NOT NULL,
    CONSTRAINT uq_dim_calidad UNIQUE (estado_validacion, codigo_error_principal)
);

-- TABLA DE HECHOS: Declaraciones Anuales
CREATE TABLE IF NOT EXISTS fact_declaraciones (
    declaracion_key BIGINT AUTO_INCREMENT PRIMARY KEY,
    contribuyente_key INT NOT NULL,
    tiempo_key INT NOT NULL,
    calidad_key INT NOT NULL,
    it_aa DECIMAL(20, 4) NULL,
    td_aa DECIMAL(20, 4) NULL,
    upaptu_c_aa DECIMAL(20, 4) NULL,
    ptu_aa DECIMAL(20, 4) NULL,
    ufe_c_aa DECIMAL(20, 4) NULL,
    pfe_c_aa DECIMAL(20, 4) NULL,
    pfea_aa DECIMAL(20, 4) NULL,
    dafpe_aa DECIMAL(20, 4) NULL,
    rfis_c_aa DECIMAL(20, 4) NULL,
    isreje_c_aa DECIMAL(20, 4) NULL,
    cfdim_aa DECIMAL(20, 4) NULL,
    rpm_aa DECIMAL(20, 4) NULL,
    orisr_aa DECIMAL(20, 4) NULL,
    impceje_c_aa DECIMAL(20, 4) NULL,
    estec_aa DECIMAL(20, 4) NULL,
    escine_aa DECIMAL(20, 4) NULL,
    estea_aa DECIMAL(20, 4) NULL,
    otroes_aa DECIMAL(20, 4) NULL,
    totes_c_aa DECIMAL(20, 4) NULL,
    ppcon_aa DECIMAL(20, 4) NULL,
    ppef_aa DECIMAL(20, 4) NULL,
    imrc_aa DECIMAL(20, 4) NULL,
    imape_aa DECIMAL(20, 4) NULL,
    imad_aa DECIMAL(20, 4) NULL,
    cfietu_aa DECIMAL(20, 4) NULL,
    imccfc_aa DECIMAL(20, 4) NULL,
    imccfe_aa DECIMAL(20, 4) NULL,
    occ_aa DECIMAL(20, 4) NULL,
    ocf_aa DECIMAL(20, 4) NULL,
    difcar_c_aa DECIMAL(20, 4) NULL,
    diffav_c_aa DECIMAL(20, 4) NULL,
    imide_aa DECIMAL(20, 4) NULL,
    impsun_aa DECIMAL(20, 4) NULL,
    isrcareje_c_aa DECIMAL(20, 4) NULL,
    isrpeimpac_aa DECIMAL(20, 4) NULL,
    isrfaveje_c_aa DECIMAL(20, 4) NULL,
    FOREIGN KEY (contribuyente_key) REFERENCES dim_contribuyente(contribuyente_key),
    FOREIGN KEY (tiempo_key) REFERENCES dim_tiempo(tiempo_key),
    FOREIGN KEY (calidad_key) REFERENCES dim_calidad_dato(calidad_key)
);

-- =============================================
-- FASE 3: CONSULTAS DE VERIFICACIÓN Y ANÁLISIS
-- =============================================

-- ----- Consultas de Negocio para el OLTP (Punto 'c' del Proyecto) -----

-- 1. Resumen de la última ejecución del ETL
-- Proporciona una visión rápida del resultado del proceso de carga más reciente, útil para verificar el estado.
SELECT * FROM procesamientologdeclaraciones ORDER BY logid DESC LIMIT 2;

-- 2. Conteo de registros por estado de validación para una carga específica
-- Permite auditar la calidad de los datos de un archivo en particular (reemplazar [ID_LOG] con un ID real).

SELECT
    d.estadovalidacion,
    COUNT(d.id_declaracion) AS total_declaraciones
FROM declaraciones_pm_anuales d
WHERE EXISTS (
    SELECT 1 FROM erroresvalidaciondeclaraciones e
    WHERE e.id_declaracion_fk = d.id_declaracion AND e.logid_fk = 2 
) 
OR d.estadovalidacion = 'Valido'
GROUP BY d.estadovalidacion;

-- 3. Frecuencia de los tipos de error en la última carga
-- Ayuda a identificar los problemas de calidad de datos más comunes en la fuente de datos más reciente.
SELECT codigoerror, COUNT(*) AS frecuencia
FROM erroresvalidaciondeclaraciones
WHERE logid_fk = (SELECT MAX(logid) FROM procesamientologdeclaraciones)
GROUP BY codigoerror
ORDER BY frecuencia DESC;

-- 4. Muestra de registros con un error específico
-- Facilita el análisis a fondo de un problema particular, mostrando ejemplos concretos.
SELECT * FROM declaraciones_pm_anuales
WHERE id_declaracion IN (
    SELECT id_declaracion_fk FROM erroresvalidaciondeclaraciones 
    WHERE codigoerror = 'INCONSISTENCIA_UPAPTU' AND logid_fk = (SELECT MAX(logid) FROM procesamientologdeclaraciones)
) LIMIT 10;

-- 5. Verificación de duplicados cargados en la tabla OLTP
-- Es una consulta operativa clave para garantizar la integridad de los datos y que la constraint UNIQUE está funcionando.
SELECT rfc_anon, ejercicio, COUNT(*) AS numero_de_registros
FROM declaraciones_pm_anuales
GROUP BY rfc_anon, ejercicio
HAVING COUNT(*) > 1;


-- ----- Consultas de Verificación del DW (Después de ETL_DWH.py) -----
-- Verificar contenido de dim_tiempo
SELECT * FROM dim_tiempo ORDER BY ejercicio;

-- Contar el total de hechos en fact_declaraciones
SELECT COUNT(*) AS total_hechos FROM fact_declaraciones;

-- Muestra de datos de la tabla de hechos con dimensiones
SELECT 
    fd.declaracion_key,
    dc.rfc_anon,
    dt.ejercicio,
    dqd.estado_validacion,
    dqd.codigo_error_principal,
    fd.it_aa,
    fd.td_aa,
    fd.upaptu_c_aa
FROM fact_declaraciones fd
JOIN dim_contribuyente dc ON fd.contribuyente_key = dc.contribuyente_key
JOIN dim_tiempo dt ON fd.tiempo_key = dt.tiempo_key
JOIN dim_calidad_dato dqd ON fd.calidad_key = dqd.calidad_key
ORDER BY dt.ejercicio, fd.declaracion_key
LIMIT 20;


-- ----- Consultas de Negocio para el DW (Punto 'g' del Proyecto) -----

-- 1. Ingresos Acumulables Totales (IT_AA) y Resultado Fiscal Total (RFIS_c_AA) por Año:
SELECT 
    t.ejercicio,
    SUM(f.it_aa) AS TotalIngresosAcumulables,
    SUM(f.rfis_c_aa) AS TotalResultadoFiscal
FROM fact_declaraciones f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
GROUP BY t.ejercicio
ORDER BY t.ejercicio;

-- 2. Distribución de Declaraciones por Estado de Validación y Código de Error Principal, por Año:
SELECT 
    t.ejercicio,
    qd.estado_validacion,
    qd.codigo_error_principal,
    COUNT(f.declaracion_key) AS NumeroDeDeclaraciones
FROM fact_declaraciones f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
JOIN dim_calidad_dato qd ON f.calidad_key = qd.calidad_key
GROUP BY t.ejercicio, qd.estado_validacion, qd.codigo_error_principal
ORDER BY t.ejercicio, NumeroDeDeclaraciones DESC;

-- 3. Comparativa Anual del ISR del Ejercicio (ISREJE_c_AA) para Declaraciones "Válidas" vs. "INCONSISTENCIA_UPAPTU":
SELECT 
    t.ejercicio,
    qd.codigo_error_principal,
    AVG(f.isreje_c_aa) AS Promedio_ISR_Ejercicio,
    COUNT(f.declaracion_key) AS NumeroDeDeclaraciones
FROM fact_declaraciones f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
JOIN dim_calidad_dato qd ON f.calidad_key = qd.calidad_key
WHERE qd.codigo_error_principal IN ('NINGUNO', 'INCONSISTENCIA_UPAPTU')
GROUP BY t.ejercicio, qd.codigo_error_principal
ORDER BY t.ejercicio, qd.codigo_error_principal;

-- 4. Top 5 RFCs por Suma de Pérdidas Fiscales del Ejercicio (PFE_c_AA) en 2015:
SELECT 
    c.rfc_anon,
    SUM(f.pfe_c_aa) AS TotalPerdidaFiscal_2015
FROM fact_declaraciones f
JOIN dim_contribuyente c ON f.contribuyente_key = c.contribuyente_key
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
WHERE t.ejercicio = 2015 AND f.pfe_c_aa > 0
GROUP BY c.rfc_anon
ORDER BY TotalPerdidaFiscal_2015 DESC
LIMIT 5;

-- 5. Evolución del Promedio de PTU Pagada (PTU_AA) por Año (solo para quienes pagaron PTU):
SELECT 
    t.ejercicio,
    AVG(f.ptu_aa) AS Promedio_PTU_Pagada,
    COUNT(DISTINCT f.contribuyente_key) AS NumeroDeEmpresasQuePagaronPTU
FROM fact_declaraciones f
JOIN dim_tiempo t ON f.tiempo_key = t.tiempo_key
WHERE f.ptu_aa > 0 
GROUP BY t.ejercicio
ORDER BY t.ejercicio;
-- ----- Sentencias para Limpiar Datos (USAR CON PRECAUCIÓN) -----

SET SQL_SAFE_UPDATES = 0;

-- Limpiar DW
DELETE FROM fact_declaraciones;
DELETE FROM dim_calidad_dato; 
DELETE FROM dim_tiempo;       
DELETE FROM dim_contribuyente;

-- Limpiar Errores y Logs del OLTP
DELETE FROM erroresvalidaciondeclaraciones;
DELETE FROM procesamientologdeclaraciones;

-- Limpiar Tabla OLTP Principal 
DELETE FROM declaraciones_pm_anuales; 

SET SQL_SAFE_UPDATES = 1;

-- Para reiniciar los contadores AUTO_INCREMENT (opcional)
-- ALTER TABLE declaraciones_pm_anuales AUTO_INCREMENT = 1;
-- ALTER TABLE procesamientologdeclaraciones AUTO_INCREMENT = 1;
-- ALTER TABLE erroresvalidaciondeclaraciones AUTO_INCREMENT = 1;
-- ALTER TABLE dim_contribuyente AUTO_INCREMENT = 1;
-- ALTER TABLE dim_tiempo AUTO_INCREMENT = 1;
-- ALTER TABLE dim_calidad_dato AUTO_INCREMENT = 1;
-- ALTER TABLE fact_declaraciones AUTO_INCREMENT = 1;

SELECT 'Script SQL consolidado y estructurado listo para revisión y uso.' AS Estado_Final_Script;
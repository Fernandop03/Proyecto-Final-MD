<p align="center">
  <img src="https://img.shields.io/badge/Proyecto-ETL_ISR_PM-4B8BBE?style=for-the-badge&logo=python&logoColor=white" alt="Proyecto ETL ISR PM"/>
</p>

<h1 align="center">📊 ETL Declaraciones Anuales ISR Personas Morales 2015</h1>

<p align="center">
  Un pipeline de datos para cargar, transformar y analizar declaraciones fiscales 📂⚙️📈  
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.x-blue?style=flat-square&logo=python&logoColor=white">
  <img src="https://img.shields.io/badge/MySQL-Compatible-blue?style=flat-square&logo=mysql&logoColor=white">
  <img src="https://img.shields.io/badge/ETL-Automatizado-green?style=flat-square&logo=data">
</p>

# Proyecto de Carga y Transformación de Datos de Declaraciones Anuales de Personas Morales (ISR_PM)

Este repositorio contiene scripts y configuraciones para la carga, transformación y análisis de datos de declaraciones anuales del Impuesto Sobre la Renta (ISR) para Personas Morales (PM) del ejercicio 2015. El proyecto implementa un proceso ETL (Extract, Transform, Load) para mover datos desde un archivo CSV original hacia una base de datos OLTP y posteriormente a un Data Warehouse (DW) con un esquema de estrella.

## Contenido del Repositorio

* `Anuales_ISR_PM_2015.csv`: Archivo de datos brutos de las declaraciones anuales de ISR para Personas Morales del ejercicio 2015.
* `DiccionarioDatosPM_anonimizados_2010-2015.xls - Diccionario.csv`: Diccionario de datos que describe las columnas y su significado en el archivo CSV principal.
* `DiccionarioDatosPM_anonimizados_2010-2015.xls - Cifras de control.csv`: Archivo con cifras de control o metadatos relacionados con los datos.
* `OLTP.sql`: Script SQL para la creación de la base de datos transaccional (OLTP) y sus tablas, incluyendo la tabla principal `declaracionespm2015`. Define las relaciones y claves foráneas a las dimensiones del DW, lo que indica un diseño que ya contempla la integración.
* `ETL.py`: Script Python encargado del proceso de Extracción, Transformación y Carga inicial de los datos desde el archivo `Anuales_ISR_PM_2015.csv` hacia la base de datos OLTP (`DeclaracionesAnualesDB`). Incluye validaciones de datos y un sistema de log para el seguimiento del procesamiento.
* `ETL_DWH.py`: Script Python que realiza el segundo paso del proceso ETL, moviendo los datos desde la base de datos OLTP (`DeclaracionesAnualesDB`) a las tablas del Data Warehouse (DW). Este script se encarga de poblar las tablas de hechos (`fact_declaraciones`) y dimensiones (`dim_tiempo`, `dim_contribuyente`, `dim_calidad_dato`) con la lógica de transformación adecuada para el modelo estrella.
* `duplicados.py`: Script Python auxiliar para identificar y reportar posibles registros duplicados dentro del archivo CSV inicial (`Anuales_ISR_PM_2015.csv`), basándose en las columnas `RFC_ANON` y `ejercicio`.

## Estructura de la Base de Datos

El proyecto utiliza una base de datos MySQL con dos esquemas principales:

1.  **Base de Datos OLTP (`DeclaracionesAnualesDB`):**
    * **Tabla Principal:** `declaracionespm2015`
        * Almacena los datos brutos o ligeramente procesados directamente del CSV.
        * Columnas clave: `id_declaracion`, `rfc_anon`, `ejercicio`, y múltiples columnas numéricas relacionadas con los montos de la declaración (`it_aa`, `td_aa`, `upaptu_c_aa`, etc.).
        * Este script `OLTP.sql` también define las claves foráneas que eventualmente conectarán esta tabla con las dimensiones del Data Warehouse, lo que sugiere una integración planificada.

2.  **Data Warehouse (DW - Esquema Estrella):**
    * **Tabla de Hechos:** `fact_declaraciones`
        * Contiene las métricas numéricas principales de las declaraciones.
        * Claves foráneas para las tablas de dimensión.
    * **Tablas de Dimensión:**
        * `dim_tiempo`: Contiene información relacionada con la fecha (ejercicio fiscal).
        * `dim_contribuyente`: Contiene información sobre los contribuyentes (basado en `rfc_anon`).
        * `dim_calidad_dato`: Posiblemente para registrar la calidad o validez de los datos procesados.

## Requisitos

Para ejecutar estos scripts, necesitarás:

* Python 3.x
* MySQL Server (o compatible)
* Las siguientes librerías de Python:
    * `pandas`
    * `mysql-connector-python`
    * `numpy` (para `ETL_DWH.py`)

Puedes instalarlas usando pip:

```bash
pip install pandas mysql-connector-python numpy




graph TD
    A[Terminal/Git Bash en Carpeta del Proyecto] --> B[Ejecutar: python ETL.py]
    B --> C[Lectura de Anuales_ISR_PM_2015.csv]
    C --> D[Validación y Transformación de Datos]
    D --> E[Carga de Datos a la Tabla 'declaracionespm2015' en OLTP]
    E --> F[Reporte de Proceso y Errores]
    F --> G[Datos en BD OLTP Listos]
```


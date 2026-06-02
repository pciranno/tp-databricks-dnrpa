# tp-databricks-dnrpa
# 🚗 Proyecto de Ingeniería de Datos: Arquitectura Medallón - Transferencias DNRPA

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=Databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-FDEE21?style=for-the-badge&logo=apachespark&logoColor=black)
![Delta Lake](https://img.shields.io/badge/Delta_Lake-00AAD4?style=for-the-badge&logo=databricks&logoColor=white)
![SQL](https://img.shields.io/badge/SQL-4479A1?style=for-the-badge&logo=mysql&logoColor=white)


## 📌 1. Descripción General del Proyecto

Este proyecto implementa una solución de **Ingeniería de Datos End-to-End** bajo la **Arquitectura Medallón** (Bronze, Silver y Gold) en **Databricks (AWS Platform)** utilizando **Serverless Compute**. El objetivo principal es procesar, limpiar, estandarizar y modelar analíticamente los datos públicos y masivos de transferencias vehiculares de la **DNRPA** (Registro Nacional de la Propiedad del Automotor de Argentina).

El pipeline está diseñado bajo principios de producción industrial: es **idempotente**, separa estrictamente la definición de infraestructura (**DDL**) de los procesos de transformación (**DML/ETL**), aplica técnicas defensivas de calidad de datos, e integra un esquema en estrella optimizado listo para consumo mediante herramientas de Business Intelligence (BI) como Power BI.

* **Volumen Inicial (Bronze):** ~3.3 Millones de registros crudos.
* **Volumen Consolidado (Gold):** ~2.4 Millones de transacciones de transferencias normalizadas.

---

## 🏗️ 2. Arquitectura de Datos y Pipeline (Medallón)

### 🥉 Capa Bronze (Raw Layer)
* **Origen de Datos:** Archivos históricos de transferencias vehiculares en formato CSV provistos por la DNRPA.
* **Estructura:** Carga cruda 1:1 sin aplicación de esquemas estrictos (`inferSchema = false`) para garantizar que ninguna anomalía de tipado en origen interrumpa el pipeline de ingesta.
* **Almacenamiento:** Formato **Delta Lake** en `workspace.tp_dnrpa_bronze.bronze_transferencias`.

### 🥈 Capa Silver (Cleansed & Conformed Layer)
La capa Silver actúa como la frontera de calidad del dato. Se implementaron pipelines de transformación avanzados mediante **PySpark** para domar la alta inconsistencia de los registros seccionales:
1.  **Limpieza Inicial:** Remoción de columnas con alta tasa de nulos o irrelevantes para el negocio (`_rescued_data`, `titular_pais_nacimiento_id`, `titular_genero`, `titular_anio_nacimiento`).
2.  **Clasificación de Trámites:** Uso de expresiones regulares (Regex) para identificar tipos de trámite, filtrando exclusivamente los registros correspondientes a `"TRANSFERENCIA"`.
3.  **Normalización Algorítmica en 3 Niveles (Campos Críticos):**
    * *Nivel 1 (Frecuencia SQL):* Para mitigar errores de tipeo manual, se calcula el `automotor_marca_descripcion` más frecuente asociado a cada `automotor_marca_codigo`.
    * *Nivel 2 (Diccionario PySpark):* Mapeo de variantes de texto libre a nombres oficiales estandarizados (Ej: `VOLK`, `VW` ➡️ `VOLKSWAGEN`) utilizando diccionarios controlados (~50 marcas principales y ~40 tipos de vehículos como `SEDAN`, `PICK-UP`, `FURGON`).
    * *Nivel 3 (IDs Sintéticos por Bandas de Frecuencia):* Para resolver la "cola larga" de datos huérfanos o raros, se diseñó una asignación defensiva de IDs sintéticos por rangos que evita colisiones futuras con códigos oficiales:
        * **Marcas:** `9001+` (destacadas sin ID), `9999+` (raras con <= 10 registros, asignando IDs únicos incrementales sin repetición).
        * **Tipos de Vehículo:** `8001+` (destacados), `8899+` (raros).
        * **Modelos:** `7001+` (destacados), `7999+` (raros). No usa diccionario debido a la existencia de más de 10,000 variantes.
4.  **Data Quality Defensivo:**
    * Conversión de strings vacíos (`""` o `" "`) a verdaderos nulos (`NULL`) relacionales mediante funciones de `trim()` combinadas con condicionales Spark, evitando fallos en uniones analíticas posteriores.
    * Uso estricto de `TRY_CAST` en campos temporales para capturar anomalías e inserción de una columna de auditoría de procesamiento (`silver_ingestion_timestamp`).
5.  **Clave Subrogada Compuesta y Deduplicación:** Generación del campo `id_tramite` mediante la concatenación determinística de: `registro_seccional_codigo` + `automotor_marca_codigo` + `automotor_modelo_codigo` + `automotor_tipo_codigo` + `tramite_fecha`. Deduplicación final del DataFrame garantizando la unicidad transaccional.

### 🥇 Capa Gold (Curated & Modeled Layer)
Transformación del dataset plano de Silver en un **Modelo Dimensional (Esquema en Estrella)** mediante SQL nativo optimizado. El almacenamiento remueve descripciones de texto redundantes en la tabla central para minimizar el costo de almacenamiento y maximizar la velocidad de escaneo.

* **Tabla de Hechos Central (`fact_transferencias`):** Contiene 2.4M de filas transaccionales, las métricas cuantitativas (`tramite_fecha`, `automotor_anio_modelo`) y exclusivamente las Claves Foráneas (FK) que apuntan a los catálogos dimensionales.
* **Tablas de Dimensiones (Catálogos Maestros `DISTINCT`):**
    * `dim_marca`: Catálogo de marcas de vehículos (PK: `automotor_marca_codigo`).
    * `dim_tipo_vehiculo`: Agrupaciones limpias de carrocerías (PK: `automotor_tipo_codigo`).
    * `dim_modelo`: Catálogo maestro de modelos normalizados (PK: `automotor_modelo_codigo`).
    * `dim_geografia`: Desglose político-geográfico de los registros seccionales y sus provincias (PK: `registro_seccional_codigo`).

#### 📐 Capa Semántica: Vista Analítica
Para simplificar la explotación analítica y optimizar las consultas en herramientas de visualización, se implementó la vista desnormalizada `workspace.tp_dnrpa_gold.vw_transferencias_analitica`. Esta capa resuelve de forma interna y transparente todos los `LEFT JOINs` relacionales, permitiendo la exploración directa del dataset sin requerir sentencias complejas en el lado del consumidor.

---

## 📂 3. Estructura del Repositorio

El repositorio está organizado siguiendo las mejores prácticas de ingeniería de software para Big Data, aislando los entornos de definición de objetos de las tareas de orquestación diaria.

```text
📁 /
├── 📁 01_DDL/                       # Estructuras e Infraestructura (Setup Inicial)
│   ├── DDL_Bronze.sql               # Definición de esquemas y tablas crudas Delta
│   ├── DDL_Silver.sql               # Definición estricta de esquemas limpios y tipados
│   └── DDL_Gold.sql                 # Creación del Modelo Dimensional y Vista Semántica
├── 📁 02_Bronze/                    # Pipeline de Ingesta (DML)
│   └── Bronze_dnrpa.py              # Ingesta idempotente desde CSV a Delta (INSERT OVERWRITE)
├── 📁 03_Silver/                    # Pipeline de Refinamiento (PySpark)
│   └── Silver_dnrpa.py              # Procesamiento de calidad, auditoría y deduplicación
└── 📁 04_Gold/                      # Pipeline de Modelado Dimensional (DML SQL)
    ├── Gold_modelado.sql            # Construcción de Dimensiones e inserción a Fact
    └── QA_Validations.sql           # Suite de testing automatizado de calidad de datos

📊 4. Validaciones de Calidad y Gobierno del Dato (QA Suite)
La confiabilidad de la capa Gold se asegura mediante una suite automatizada de 5 pruebas de calidad integradas al pipeline:

Conteo de Registros: Asegura el correcto aprovisionamiento de las 5 entidades del modelo en estrella (2.4M de registros en hechos).

Unicidad de Claves Primarias (PKs): Valida la ausencia absoluta de duplicados en los catálogos dimensionales maestros.

Análisis de Nulos: Certifica tasa de 0% de valores nulos en columnas de enlace de negocio (PKs y FKs).

Integridad Referencial Estricta: Comprobación matemática de 0 registros huérfanos; cada clave de la tabla fact_transferencias posee una correspondencia exacta en su dimensión asociada.

Reconciliación Operacional (Silver vs Gold): Monitoreo de desvíos en el volumen de datos. El cálculo implementa lógica defensiva basada en NULLIF(registros_silver, 0) para mitigar de forma proactiva excepciones por división por cero en ejecuciones con tablas vacías.

🚀 5. Flujo de Despliegue y Ejecución
Fase de Aprovisionamiento (Ejecución Única)
Antes del primer procesamiento de datos, se deben ejecutar de forma secuencial los notebooks alojados en la carpeta 01_DDL/ para inicializar físicamente los contenedores en el catálogo de Databricks:

Correr 01_DDL/DDL_Bronze.sql

Correr 01_DDL/DDL_Silver.sql

Correr 01_DDL/DDL_Gold.sql

Pipeline de Carga Recurrente (Orquestación Automatizada)
Una vez montada la infraestructura vacía, las cargas subsiguientes se ejecutan utilizando sentencias eficientes de tipo INSERT OVERWRITE que reescriben los datos sin destruir las definiciones físicas ni los comentarios de metadatos de las tablas:

02_Bronze_dnrpa: Lee los nuevos CSV e impacta la capa cruda.

02_Silver_dnrpa: Consume Bronze, aplica el motor de PySpark (diccionarios + rangos de IDs sintéticos), estampa el timestamp y deduplica.

04_Gold_modelado: Actualiza de forma atómica los catálogos de dimensiones y la tabla de hechos.

QA_Validations: Verifica los umbrales de integridad. El dataset queda consolidado y disponible a través de vw_transferencias_analitica para consumo inmediato en Power BI.

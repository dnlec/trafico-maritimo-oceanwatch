# Databricks notebook source
# MAGIC %md
# MAGIC # Requisito 1 Ingesta
# MAGIC Descargamos los 7 dias de datos AIS (NOAA, 2023-06-01 a 06-07) al Volume, los descomprimimos y losleemos como un unico DataFrame con esquema explicito. Tres pasos: **descarga + verificacion**, **lecturatipada** y una **comprobacion** rapida de que la ingesta quedo completa.

# COMMAND ----------

from databricks.connect import DatabricksSession
from databricks.sdk import WorkspaceClient

# Python de cada celda corre en el laptop. Spark corre en serverless.
# /Volumes no esta montado localmente: los archivos se leen con Spark o con la Files API.
spark = DatabricksSession.builder.getOrCreate()
w = WorkspaceClient()

# Datos de la Parte 1. El target del bundle (catalog workspace) es otro lugar.
CATALOG = "proyecto_datos"
SCHEMA = "default"
VOLUME_ROOT = f"/Volumes/{CATALOG}/{SCHEMA}/data"
AIS_DIR = f"{VOLUME_ROOT}/ais_data"
WPI_PATH = f"{VOLUME_ROOT}/UpdatedPub150.csv"
PARQUET_PATH = f"{VOLUME_ROOT}/ais_parquet/"
DELTA_TABLE = f"{CATALOG}.{SCHEMA}.ais_positions_delta"

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SCHEMA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Descarga, verificacion e ingesta
# MAGIC
# MAGIC Por cada URL: descarga con reintentos (`download_with_retries`), verifica que el zip no este corrupto
# MAGIC (`verify_zip_integrity`) y sube los CSV al Volume. El zip se abre en el laptop (Databricks Connect no
# MAGIC monta `/Volumes` localmente) y cada CSV se envia con la Files API. Si los 7 CSV ya estan, no se
# MAGIC vuelve a descargar. Si una descarga o la integridad falla, se detiene con error.

# COMMAND ----------

import os
import tempfile
import time
import zipfile
import requests
from pathlib import Path

from databricks.sdk.errors import DatabricksError

urls = [
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_01.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_02.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_03.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_04.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_05.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_06.zip",
    "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2023/AIS_2023_06_07.zip",
]


def list_volume_csvs(directory: str) -> list[str]:
    """CSV directamente en un directorio del Volume. Lista vacia si el directorio no existe."""
    try:
        entries = w.files.list_directory_contents(directory)
    except DatabricksError as exc:
        if exc.error_code == "NOT_FOUND":
            return []
        raise
    return sorted(
        entry.path
        for entry in entries
        if not entry.is_directory and (entry.path or "").endswith(".csv")
    )


def ensure_volume_dir(directory: str) -> None:
    try:
        w.files.create_directory(directory)
    except DatabricksError as exc:
        if exc.error_code != "RESOURCE_ALREADY_EXISTS":
            raise


existing_csvs = list_volume_csvs(AIS_DIR)
if len(existing_csvs) >= 7:
    print(f"Se encontraron {len(existing_csvs)} archivos CSV ya presentes en {AIS_DIR}.")
    print("Omitiendo descarga y extraccion. Los datos ya estan listos.")
else:
    ensure_volume_dir(AIS_DIR)

    def download_with_retries(url, dest_path, max_retries=3, delay=5):
        """Descarga un archivo con reintentos automaticos."""
        for attempt in range(1, max_retries + 1):
            try:
                print(f"Downloading {os.path.basename(dest_path)} (Attempt {attempt}/{max_retries})...")
                response = requests.get(url, stream=True, timeout=60)
                response.raise_for_status()

                with open(dest_path, "wb") as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        f.write(chunk)
                return True
            except Exception as e:
                print(f"Attempt {attempt} failed: {e}")
                if attempt < max_retries:
                    time.sleep(delay)
                else:
                    raise RuntimeError(f"Failed to download {url} after {max_retries} attempts.")

    def verify_zip_integrity(zip_filepath):
        """Verifica que el zip sea valido y no este corrupto."""
        print(f"Verifying integrity of {os.path.basename(zip_filepath)}...")
        if not zipfile.is_zipfile(zip_filepath):
            return False

        with zipfile.ZipFile(zip_filepath, "r") as zf:
            # testzip devuelve None si no hay archivos danados
            bad_file = zf.testzip()
            return bad_file is None

    # El zip se abre en el laptop. Cada CSV extraido se sube al Volume.
    with tempfile.TemporaryDirectory() as tmp:
        tmp_dir = Path(tmp)
        for url in urls:
            zip_name = url.split("/")[-1]
            zip_path = tmp_dir / zip_name

            download_with_retries(url, zip_path, max_retries=3)

            if not verify_zip_integrity(zip_path):
                raise ValueError(f"Integrity check failed for {zip_name}. The file is corrupted.")
            print(f"Integrity verified for {zip_name}.")

            print(f"Extracting {zip_name}...")
            with zipfile.ZipFile(zip_path, "r") as zip_ref:
                zip_ref.extractall(tmp_dir)
            zip_path.unlink()

            for csv_path in sorted(tmp_dir.glob("*.csv")):
                remote = f"{AIS_DIR}/{csv_path.name}"
                print(f"Uploading {csv_path.name} -> {remote}")
                with csv_path.open("rb") as fh:
                    w.files.upload(remote, fh, overwrite=True)
                csv_path.unlink()

    print("All files downloaded, verified, and extracted successfully.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lectura con esquema explicito
# MAGIC
# MAGIC Leemos todos los CSV con un esquema declarado (`ais_schema`) en vez de inferirlo: garantiza los tipos
# MAGIC correctos (p. ej. `BaseDateTime` como `timestamp`, `LAT`/`LON` como `double`) y evita una pasada extra
# MAGIC de inferencia sobre 60 M de filas.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

BASE = AIS_DIR

ais_schema = """
    MMSI string,
    BaseDateTime timestamp,
    LAT double,
    LON double,
    SOG double,
    COG double,
    Heading double,
    VesselName string,
    IMO string,
    CallSign string,
    VesselType int,
    Status int,
    Length double,
    Width double,
    Draft double,
    Cargo string,
    TransceiverClass string
"""

df_ais = (
    spark.read
    .option("header", "true")
    .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
    .schema(ais_schema)
    .csv(f"{BASE}/*.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Comprobacion de la ingesta
# MAGIC
# MAGIC Una vista de 10 filas para inspeccionar la estructura y el conteo total de registros, que confirma que
# MAGIC los 7 dias se cargaron (~60.5 M de posiciones).

# COMMAND ----------

display(df_ais.limit(10))

# COMMAND ----------

df_ais.count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Requisito 2 Exploracion y perfilamiento
# MAGIC
# MAGIC **Objetivo.** Caracterizar el dataset AIS de la semana (7 dias) antes de responder preguntas de
# MAGIC negocio o diseñar el almacenamiento: cuanto volumen hay, como se comporta dia a dia, que tipos de
# MAGIC embarcación lo componen y, sobre todo, que problemas de calidad tiene, para definir las reglas de
# MAGIC limpieza de la Entrega 2.
# MAGIC
# MAGIC El analisis se organiza en cuatro pasos:
# MAGIC
# MAGIC 1. **Preparacion** de columnas derivadas que se reutilizan en todo el requisito (`EventDate` y una
# MAGIC    marca de duplicados).
# MAGIC 2. **Volumen** general y por dia (total de registros, buques unicos, actividad diaria).
# MAGIC 3. **Perfilamiento** de embarcaciones: distribucion por tipo de buque (con velocidad media) y
# MAGIC    distribucion de dimensiones fisicas (eslora, manga, calado).
# MAGIC 4. **Diagnostico de calidad**: una tabla unica que cuantifica cada regla de validacion sobre el
# MAGIC    total de filas. Es el insumo directo de la Entrega 2.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 1: Columnas derivadas de apoyo
# MAGIC
# MAGIC Dos columnas que reutilizan los pasos siguientes:
# MAGIC
# MAGIC - **`EventDate`**: la fecha (sin hora) de cada emision, para los cortes diarios.
# MAGIC - **`is_duplicate`**: marca de duplicado exacto. Definimos duplicado como una misma emision fisica,
# MAGIC   es decir, la combinacion `(MMSI, BaseDateTime, LAT, LON)` que aparece mas de una vez. En lugar de un
# MAGIC   `row_number()` sobre una ventana, cuyo orden dentro del grupo es arbitrario y confunde la lectura,
# MAGIC   contamos cuantas veces aparece cada combinacion (`count` sobre la particion) y marcamos como
# MAGIC   duplicada toda fila cuyo grupo tenga mas de un registro. Es equivalente en el conteo y autoexplicativo.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Fecha del evento (sin hora) para los cortes diarios
df_prepared = df_ais.withColumn("EventDate", F.to_date("BaseDateTime"))

# Marca de duplicado exacto: misma emision fisica repetida.
# Duplicado = combinacion (MMSI, BaseDateTime, LAT, LON) que aparece mas de una vez.
# Contamos el tamano de cada grupo y marcamos las filas cuyo grupo tiene mas de 1 registro.
dup_window = Window.partitionBy("MMSI", "BaseDateTime", "LAT", "LON")
df_flagged = df_prepared.withColumn(
    "is_duplicate",
    F.count("*").over(dup_window) > 1
)

# Conteo del hallazgo: filas marcadas como duplicadas (todas las copias del grupo)
# y numero de emisiones a eliminar (una copia por grupo se conserva).
dup_rows = df_flagged.filter(F.col("is_duplicate")).count()
dup_groups = (
    df_flagged.filter(F.col("is_duplicate"))
    .select("MMSI", "BaseDateTime", "LAT", "LON").distinct().count()
)
print(f"Filas en grupos duplicados: {dup_rows:,}")
print(f"Grupos duplicados (emisiones unicas afectadas): {dup_groups:,}")
print(f"Filas a eliminar para dejar una copia por grupo: {dup_rows - dup_groups:,}")

# Vista rapida de la estructura resultante
display(df_flagged.select("MMSI", "BaseDateTime", "LAT", "LON", "EventDate", "is_duplicate").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 2: Volumen general y actividad diaria
# MAGIC
# MAGIC Cuantificamos el tamano del dataset y su comportamiento en el tiempo:
# MAGIC
# MAGIC - **Total de registros** y **buques unicos** (MMSI distintos) en la semana completa.
# MAGIC - **Posiciones y buques activos por dia**, para ver si la actividad es estable o tiene picos.

# COMMAND ----------

# Total de registros y buques unicos en la semana
total_records = df_ais.count()
total_unique_vessels = df_ais.select("MMSI").distinct().count()

print(f"Total de registros AIS: {total_records:,}")
print(f"Buques unicos (MMSI distintos): {total_unique_vessels:,}")

# Posiciones y buques unicos por dia
daily_summary = (
    df_flagged
    .groupBy("EventDate")
    .agg(
        F.count("*").alias("Total_Positions"),
        F.countDistinct("MMSI").alias("Unique_Vessels")
    )
    .orderBy("EventDate")
)

display(daily_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 3: Perfilamiento de embarcaciones
# MAGIC
# MAGIC Dos vistas del "quien" del dataset:
# MAGIC
# MAGIC - **Distribucion por tipo de buque** (`VesselType`): numero de posiciones, buques unicos y velocidad
# MAGIC   media (`SOG`) por tipo, ordenado por volumen de posiciones. El catalogo de tipos AIS permite luego
# MAGIC   traducir cada codigo (por ejemplo 31 = remolque, 70 = carga, 80 = tanquero).
# MAGIC - **Distribucion de dimensiones fisicas** (eslora, manga, calado) via percentiles p25/p50/p75/p99,
# MAGIC   para conocer el rango de tamanos y detectar valores extremos.

# COMMAND ----------

# Distribucion por tipo de buque (todos los tipos, ordenados por volumen)
vessel_type_dist = (
    df_ais
    .groupBy("VesselType")
    .agg(
        F.count("*").alias("Position_Count"),
        F.countDistinct("MMSI").alias("Unique_Vessels"),
        F.round(F.avg("SOG"), 2).alias("Avg_SOG_Knots")
    )
    .orderBy(F.desc("Position_Count"))
)
display(vessel_type_dist)

# Distribucion de dimensiones fisicas (Length, Width, Draft)
# Cada arreglo trae los percentiles en orden: [p25, p50 (mediana), p75, p99]
size_metrics = df_ais.select(
    F.expr("percentile_approx(Length, array(0.25, 0.50, 0.75, 0.99))").alias("Length_p25_p50_p75_p99"),
    F.expr("percentile_approx(Width,  array(0.25, 0.50, 0.75, 0.99))").alias("Width_p25_p50_p75_p99"),
    F.expr("percentile_approx(Draft,  array(0.25, 0.50, 0.75, 0.99))").alias("Draft_p25_p50_p75_p99")
)
display(size_metrics)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 4: Diagnostico de calidad de datos
# MAGIC
# MAGIC Una sola pasada de agregacion condicional cuenta, para cada regla de validacion, cuantas filas la
# MAGIC incumplen y su porcentaje sobre el total. Reunir todas las reglas en un unico `select` evita recorrer
# MAGIC los 60 M de filas una vez por regla. Las reglas cubren:
# MAGIC
# MAGIC - **Posiciones fuera de rango**: `LAT` fuera de [-90, 90] o `LON` fuera de [-180, 180], o exactamente 0.0.
# MAGIC - **Velocidades (`SOG`) anomalas**: > 60 nudos (fisicamente imposible para trafico comercial) o negativas.
# MAGIC - **MMSI anomalos**: identificadores que no son de 9 digitos o codigos de prueba (`000000000`).
# MAGIC - **Heading**: valor 511 ("no disponible" segun la norma ITU) y valores fuera de [0, 359].
# MAGIC - **Dimensiones invalidas**: `Length` o `Width` <= 0.
# MAGIC - **Duplicados**: la marca `is_duplicate` del Paso 1 (cuenta *todas* las filas de un grupo repetido, no solo las copias sobrantes).

# COMMAND ----------

quality_diagnostics = df_flagged.select(
    # Total de filas, para calcular porcentajes
    F.count("*").alias("total_rows"),

    # Posiciones fuera de rango (LAT valida: -90 a 90, LON valida: -180 a 180)
    F.sum(F.when((F.col("LAT") < -90) | (F.col("LAT") > 90) | (F.col("LAT") == 0.0), 1).otherwise(0)).alias("invalid_lat_count"),
    F.sum(F.when((F.col("LON") < -180) | (F.col("LON") > 180) | (F.col("LON") == 0.0), 1).otherwise(0)).alias("invalid_lon_count"),

    # Anomalias de velocidad (SOG). Mayor a 60 nudos es imposible para naves comerciales. SOG menor a 0 es invalido
    F.sum(F.when(F.col("SOG") > 60.0, 1).otherwise(0)).alias("impossible_speed_count"),
    F.sum(F.when(F.col("SOG") < 0.0, 1).otherwise(0)).alias("negative_speed_count"),

    # MMSI anomalos: deben ser identificadores de 9 digitos. 000000000 o longitudes distintas son invalidos
    F.sum(F.when((F.length(F.col("MMSI")) != 9) | (F.col("MMSI") == "000000000") | F.col("MMSI").rlike("^0+$"), 1).otherwise(0)).alias("anomalous_mmsi_count"),

    # Anomalias de rumbo (Heading valido: 0 a 359 grados. 511 indica "no disponible")
    F.sum(F.when(F.col("Heading") == 511.0, 1).otherwise(0)).alias("heading_not_available_count"),
    F.sum(F.when((F.col("Heading") < 0.0) | ((F.col("Heading") > 359.0) & (F.col("Heading") != 511.0)), 1).otherwise(0)).alias("invalid_heading_count"),

    # Dimensiones invalidas (Length <= 0 o Width <= 0)
    F.sum(F.when((F.col("Length") <= 0) | (F.col("Width") <= 0), 1).otherwise(0)).alias("invalid_dimensions_count"),

    # Duplicados exactos (marca del Paso 1)
    F.sum(F.when(F.col("is_duplicate"), 1).otherwise(0)).alias("duplicate_records_count")
)

# Reestructurar a una tabla legible: una fila por regla
diag_row = quality_diagnostics.collect()[0]
total = diag_row["total_rows"]

def pct(n):
    return (n / total) * 100

quality_summary = spark.createDataFrame([
    ("Latitud invalida (|LAT| > 90 o 0.0)", diag_row["invalid_lat_count"], pct(diag_row["invalid_lat_count"])),
    ("Longitud invalida (|LON| > 180 o 0.0)", diag_row["invalid_lon_count"], pct(diag_row["invalid_lon_count"])),
    ("Velocidad imposible (SOG > 60 nudos)", diag_row["impossible_speed_count"], pct(diag_row["impossible_speed_count"])),
    ("Velocidad negativa (SOG < 0)", diag_row["negative_speed_count"], pct(diag_row["negative_speed_count"])),
    ("MMSI anomalo (longitud != 9 o dummy)", diag_row["anomalous_mmsi_count"], pct(diag_row["anomalous_mmsi_count"])),
    ("Heading no disponible (511.0)", diag_row["heading_not_available_count"], pct(diag_row["heading_not_available_count"])),
    ("Heading fuera de rango", diag_row["invalid_heading_count"], pct(diag_row["invalid_heading_count"])),
    ("Dimensiones invalidas (Length/Width <= 0)", diag_row["invalid_dimensions_count"], pct(diag_row["invalid_dimensions_count"])),
    ("Duplicados (MMSI + Timestamp + Coordenadas)", diag_row["duplicate_records_count"], pct(diag_row["duplicate_records_count"]))
], ["Quality_Issue", "Affected_Rows", "Percentage_Of_Total"])

display(quality_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 5: Auditoria de nulos (completitud por columna)
# MAGIC Hay que saber en que columnas se puede confiar. Contamos, en una sola pasada, cuantos valores nulos tiene cada una de las 17 columnas y su porcentaje sobre el total. Columnas con muchos nulos (p. ej. `IMO`, `Cargo`, `Draft`) no sirven como llaves ni como features sin imputacion. Columnas casi completas (`MMSI`, `BaseDateTime`, `LAT`, `LON`) son las confiables.

# COMMAND ----------

# Conteo de nulos por columna, en una sola pasada sobre los 60 M de filas.
# Se cuenta como nulo el valor NULL y, en columnas numericas, tambien NaN.
cols = df_ais.columns

null_exprs = [
    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in cols
]
null_row = df_ais.select(null_exprs).collect()[0]
total = df_ais.count()

null_summary = spark.createDataFrame(
    [(c, int(null_row[c]), (null_row[c] / total) * 100) for c in cols],
    ["Columna", "Nulos", "Porcentaje_Nulos"]
).orderBy(F.desc("Nulos"))

display(null_summary)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 6: Consistencia de atributos estaticos por buque (MMSI)
# MAGIC Un mismo buque (`MMSI`) deberia reportar **siempre** el mismo tipo y las mismas dimensiones durante la semana, porque son atributos estaticos. Cuando un `MMSI` reporta mas de un `VesselType` o mas de una eslora/manga, hay una inconsistencia: reprogramacion del transpondedor, reutilizacion de MMSI entre barcos, o error de captura. El hallazgo decide si podemos confiar en los atributos estaticos o si hay que resolver conflictos, por ejemplo, tomar el valor mas frecuente por MMSI. Contamos cuantos MMSI presentan cada tipo de inconsistencia.

# COMMAND ----------

# Para cada MMSI, cuantos valores DISTINTOS toma cada atributo estatico
mmsi_consistency = (
    df_ais
    .groupBy("MMSI")
    .agg(
        F.countDistinct("VesselType").alias("distinct_types"),
        F.countDistinct("Length").alias("distinct_lengths"),
        F.countDistinct("Width").alias("distinct_widths"),
        F.countDistinct("VesselName").alias("distinct_names")
    )
)

resumen_consistencia = mmsi_consistency.select(
    F.count("*").alias("total_mmsi"),
    F.sum(F.when(F.col("distinct_types") > 1, 1).otherwise(0)).alias("mmsi_varios_tipos"),
    F.sum(F.when(F.col("distinct_lengths") > 1, 1).otherwise(0)).alias("mmsi_varias_esloras"),
    F.sum(F.when(F.col("distinct_widths") > 1, 1).otherwise(0)).alias("mmsi_varias_mangas"),
    F.sum(F.when(F.col("distinct_names") > 1, 1).otherwise(0)).alias("mmsi_varios_nombres")
).collect()[0]

tm = resumen_consistencia["total_mmsi"]

def pct(n):
    return (n / tm) * 100

consistency_summary = spark.createDataFrame([
    ("MMSI con mas de un VesselType", resumen_consistencia["mmsi_varios_tipos"], pct(resumen_consistencia["mmsi_varios_tipos"])),
    ("MMSI con mas de una eslora (Length)", resumen_consistencia["mmsi_varias_esloras"], pct(resumen_consistencia["mmsi_varias_esloras"])),
    ("MMSI con mas de una manga (Width)", resumen_consistencia["mmsi_varias_mangas"], pct(resumen_consistencia["mmsi_varias_mangas"])),
    ("MMSI con mas de un VesselName", resumen_consistencia["mmsi_varios_nombres"], pct(resumen_consistencia["mmsi_varios_nombres"])),
], ["Inconsistencia", "MMSI_Afectados", "Porcentaje_De_MMSI"])

print(f"Total de MMSI distintos: {tm:,}")
display(consistency_summary)

# Ejemplos concretos: los MMSI que mas tipos distintos reportan
display(
    mmsi_consistency
    .filter(F.col("distinct_types") > 1)
    .orderBy(F.desc("distinct_types"))
    .limit(10)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso 7: Anomalias adicionales (temporales y de catalogo)
# MAGIC
# MAGIC Dos inconsistencias baratas de medir y valiosas:
# MAGIC
# MAGIC - **Posiciones imposibles en el tiempo**: un mismo `(MMSI, BaseDateTime)` con **coordenadas distintas**
# MAGIC   significa que el buque estaria en dos lugares a la vez en el mismo instante. Es un conflicto distinto
# MAGIC   del duplicado exacto del Paso 1 (mismo instante, distinto lugar) y hay que resolverlo antes de calcular
# MAGIC   trayectorias o distancias.
# MAGIC - **`VesselType` fuera de catalogo**: los tipos AIS validos van de 0 a 99. Contamos las filas con
# MAGIC   `VesselType` nulo o fuera de ese rango (en los datos aparecen codigos como 107, 136, 200).

# COMMAND ----------

# Posiciones imposibles en el tiempo: mismo (MMSI, BaseDateTime) con coordenadas distintas
time_window = Window.partitionBy("MMSI", "BaseDateTime")
df_time = df_ais.withColumn("distinct_coords", F.size(F.collect_set(F.concat_ws(",", "LAT", "LON")).over(time_window)))
impossible_time = df_time.filter(F.col("distinct_coords") > 1).count()

# VesselType fuera del catalogo AIS (valido 0-99) o nulo
invalid_type = df_ais.filter(
    F.col("VesselType").isNull() | (F.col("VesselType") < 0) | (F.col("VesselType") > 99)
).count()

# Volumen de filas con MMSI = 0 (no atribuibles a un buque)
mmsi_cero = df_ais.filter((F.col("MMSI") == "0") | F.col("MMSI").rlike("^0+$")).count()

total = df_ais.count()
anomalias = spark.createDataFrame([
    ("Posiciones imposibles en el tiempo (mismo MMSI+instante, distinto lugar)", impossible_time, (impossible_time / total) * 100),
    ("VesselType fuera de catalogo (nulo o fuera de 0-99)", invalid_type, (invalid_type / total) * 100),
    ("MMSI = 0 (no atribuible a un buque)", mmsi_cero, (mmsi_cero / total) * 100),
], ["Anomalia", "Filas_Afectadas", "Porcentaje_Del_Total"])

display(anomalias)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Diagnostico y hallazgos principales
# MAGIC
# MAGIC **1. Volumen general y comportamiento diario**
# MAGIC
# MAGIC - Total de registros procesados: **60,533,559** posiciones.
# MAGIC - Buques unicos en la semana: **31,871** MMSI distintos.
# MAGIC - Actividad diaria muy consistente: entre **8.0 y 9.0 millones** de posiciones por dia, con un promedio cercano a **20,000** buques activos diariamente.
# MAGIC
# MAGIC **2. Perfilamiento de embarcaciones y dimensiones**
# MAGIC
# MAGIC - Tipos dominantes: el tipo **31 (Towing/Tug)** y el tipo **37 (Pleasure Craft)** suman mas de **31 millones** de posiciones. Los buques mercantes y tanqueros (tipos **70** y **80**) registran velocidades medias mucho mayores (**5.8 a 6.4 nudos**) que las embarcaciones menores (**1.6 a 1.9 nudos**).
# MAGIC - Distribucion de tamano: la mediana es **22 m** de eslora (Length), **8 m** de manga (Width) y **3.7 m** de calado (Draft). En el percentil 99 aparecen embarcaciones de hasta **323 m** de eslora y **17 m** de calado (portacontenedores y grandes tanqueros).
# MAGIC
# MAGIC **3. Completitud por columna (Paso 5)**
# MAGIC
# MAGIC El nucleo dinamico del mensaje AIS esta **100% completo**: `MMSI`, `BaseDateTime`, `LAT`, `LON`, `SOG`, `COG`, `Heading` y `TransceiverClass` no tienen nulos. Los nulos se concentran en los atributos estaticos/opcionales:
# MAGIC
# MAGIC | Columna | % nulos | Implicacion para la Entrega 2 |
# MAGIC |---|---:|---|
# MAGIC | `Draft` | 64.4% | Casi inutilizable como feature sin imputacion; solo confiable para el subconjunto que lo reporta. |
# MAGIC | `IMO` | 42.8% | No sirve como llave universal de buque; usar `MMSI`. |
# MAGIC | `Status` | 33.0% | Estado de navegacion ausente en 1/3 de las filas. |
# MAGIC | `Cargo` | 32.9% | Ademas suele igualar a `VesselType`; no es tonelaje. |
# MAGIC | `CallSign` | 16.8% | Identificador secundario incompleto. |
# MAGIC | `Width` / `Length` | 15.2% / 6.1% | Dimensiones parcialmente ausentes; afecta perfiles de tamano. |
# MAGIC
# MAGIC **4. Diagnostico de calidad y anomalias (Pasos 4, 6 y 7)**
# MAGIC
# MAGIC | Regla / anomalia | Filas o MMSI | % | Lectura |
# MAGIC |---|---:|---:|---|
# MAGIC | Heading no disponible (511.0) | 33,535,628 | 55.40% | No es corrupcion: la norma ITU usa 511 cuando el barco no tiene girocompas o esta estatico. Tratar como categoria nula, no numerica. |
# MAGIC | Dimensiones invalidas (Length/Width <= 0) | 2,284,033 | 3.77% | Eslora o manga <= 0; filtrar o imputar. |
# MAGIC | VesselType fuera de catalogo (nulo o fuera de 0-99) | 215,721 | 0.356% | 209,863 nulos + codigos como 107/136/200 que no existen en el catalogo AIS. Normalizar o marcar como "desconocido". |
# MAGIC | Velocidad imposible (SOG > 60 nudos) | 160,613 | 0.265% | Picos de ruido del GPS; descartar. |
# MAGIC | MMSI anomalo (longitud != 9 o dummy) | 49,897 | 0.082% | Identificadores no validos. |
# MAGIC | MMSI = 0 (no atribuible a un buque) | 978 | 0.0016% | Subconjunto de los MMSI anomalos; sin buque asociado. |
# MAGIC | Posiciones imposibles en el tiempo (mismo MMSI+instante, distinto lugar) | 544 | 0.0009% | Un buque no puede estar en dos lugares a la vez; resolver antes de calcular trayectorias. |
# MAGIC | Duplicados exactos (MMSI + Timestamp + Coordenadas) | 2,800 | 0.0046% | Todas las filas de grupos repetidos: ~1,400 pares, asi que basta eliminar 1,400 copias sobrantes. |
# MAGIC | Coordenadas fuera de rango | 0 | 0.00% | Excelente calidad geometrica: sin LAT/LON fuera de rango global. |
# MAGIC
# MAGIC **5. Consistencia de atributos estaticos por MMSI (Paso 6): sin conflictos**
# MAGIC
# MAGIC Contra lo que suele ocurrir en feeds AIS, en esta semana **ningun MMSI** reporta mas de un `VesselType`, ni mas de una eslora, manga o nombre (0 de 31,871 en las cuatro pruebas). Es un resultado **positivo y verificado**: los atributos estaticos son internamente consistentes por buque, asi que en la Entrega 2 se pueden tomar directamente (no hace falta resolver el valor "correcto" por MMSI). El unico problema de `VesselType` es de **catalogo** (nulos/codigos invalidos), no de **inconsistencia** entre emisiones del mismo buque.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC El dataset tiene **alta integridad estructural**: el nucleo dinamico del mensaje (posicion, tiempo, velocidad, rumbo) esta **100% completo y sin coordenadas fuera de rango**, y los atributos estaticos son **consistentes por buque** (ningun MMSI cambia de tipo o dimensiones). Los problemas se concentran en columnas opcionales muy incompletas y en unos pocos valores centinela/ruido.
# MAGIC
# MAGIC Reglas de limpieza para la Entrega 2, ordenadas por impacto:
# MAGIC
# MAGIC 1. **Heading = 511** (55.4%): tratar como categoria nula / no disponible, no como valor numerico.
# MAGIC 2. **Columnas con muchos nulos** (`Draft` 64%, `IMO` 43%, `Status`/`Cargo` 33%): decidir por columna entre descartarla, imputarla o usarla solo en el subconjunto que la reporta. No usar `IMO` como llave; usar `MMSI`.
# MAGIC 3. **Dimensiones <= 0** (3.8%): filtrar o imputar.
# MAGIC 4. **VesselType fuera de catalogo** (0.36%): normalizar a un valor "desconocido" en vez de tratar codigos invalidos como categorias reales.
# MAGIC 5. **Velocidades > 60 nudos** (0.27%): descartar por ruido de GPS.
# MAGIC 6. **MMSI no validos / = 0, duplicados exactos y posiciones imposibles en el tiempo**: eliminar; son volumen bajo pero rompen los calculos de trayectoria.
# MAGIC
# MAGIC Todas las decisiones se justifican con la evidencia cuantitativa de los Pasos 4 a 7, no con supuestos.

# COMMAND ----------

# MAGIC %md
# MAGIC # Requisito 3 Preguntas de negocio

# COMMAND ----------

# MAGIC %md
# MAGIC #### a. ¿Cuántos buques distintos transmitieron cada día? Comparen el conteo exacto con approx_count_distinct y argumenten cuál usarían en producción.

# COMMAND ----------

unique_ships_day = (
    df_ais.withColumn("day", F.to_date("BaseDateTime"))
    .groupBy("day")
    .agg(F.countDistinct("MMSI").alias("unique_vessels"))
)
display(unique_ships_day)
unique_ships_day.explain("formatted")

# COMMAND ----------

approx_unique_ships_day = (
    df_ais.withColumn("day", F.to_date("BaseDateTime"))
    .groupBy("day")
    .agg(F.approx_count_distinct("MMSI").alias("unique_vessels"))
)
display(approx_unique_ships_day)
approx_unique_ships_day.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC El conteo difiere por poco para los dias. En produccion se usa el valor aproximado porque la importancia de este dato no justifica el precio de computo por un valor exacto. La aproximacion es suficiente para un reporte general.

# COMMAND ----------

# MAGIC %md
# MAGIC #### b. ¿Qué tipos de buque generan más tráfico? Top 10 por número de posiciones, con velocidad media por tipo (usen el catálogo de tipos).

# COMMAND ----------

rank_vessel_type = (
    df_ais.groupBy("VesselType")
    .agg(
        F.count("*").alias("count"),
        F.avg("SOG").alias("average_speed")
    )
    .orderBy(F.desc("count"))
    .limit(10)
)

display(rank_vessel_type)
rank_vessel_type.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC **Respuesta.** Los tipos que generan más tráfico (por número de posiciones) son el **31 (Towing)**
# MAGIC con 16.5 M y el **37 (Pleasure Craft / remolcadores menores)** con 14.5 M; juntos concentran más de
# MAGIC la mitad de las emisiones. En velocidad media se ve el patrón esperado: los tipos de carga y tanqueros
# MAGIC —**70 (Cargo, 6.37 nudos)** y **80 (Tanker, 5.87 nudos)**— navegan sustancialmente más rápido que las
# MAGIC embarcaciones costeras y de servicio, que rondan 1.6–2.5 nudos. Es decir, el mayor **volumen** de
# MAGIC mensajes lo aportan embarcaciones lentas y muy numerosas, mientras que la mayor **velocidad** la
# MAGIC aportan los buques mercantes de línea, menos numerosos pero de tránsito continuo.

# COMMAND ----------

# MAGIC %md
# MAGIC #### c. ¿Qué 10 buques recorrieron más distancia durante la semana? (orden por buque y tiempo, distancia entre posiciones consecutivas con haversine).

# COMMAND ----------

vessel_window = Window.partitionBy("MMSI").orderBy("BaseDateTime")
df = (
    df_ais.withColumn("prev_lat", F.lag("LAT").over(vessel_window))
    .withColumn("prev_lon", F.lag("LON").over(vessel_window))
)

lat1 = F.radians(F.col("prev_lat"))
lon1 = F.radians(F.col("prev_lon"))
lat2 = F.radians(F.col("LAT"))
lon2 = F.radians(F.col("LON"))

dlat = lat2 - lat1
dlon = lon2 - lon1

a = F.sin(dlat / 2) ** 2 + F.cos(lat1) * F.cos(lat2) * F.sin(dlon / 2) ** 2

r_km = 6371.0

distance_km = F.when(
    F.col("prev_lat").isNotNull() & F.col("prev_lon").isNotNull(),
    2 * r_km * F.asin(F.sqrt(a))
).otherwise(0.0)

df = df.withColumn("distance", distance_km)

top_10_distance_vessels = (
    df.groupBy("MMSI")
    .agg(F.sum("distance").alias("total_distance_km"))
    .orderBy(F.desc("total_distance_km"))
    .limit(10)
)

display(top_10_distance_vessels)
top_10_distance_vessels.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Jusitificacion
# MAGIC **Puntos claves del plan**:
# MAGIC + Column Pruning
# MAGIC + hashpartitioning(MMSI) y ordenamiento local
# MAGIC + Top-K eficiente
# MAGIC
# MAGIC La consulta se hizo de esta manera porque usa expresiones nativas para calcular la formula de Haversine y se aplico Window.partitionBy("MMSI").orderBy("BaseDateTime") junto con un limit(10). El plan de ejecucion confirma que el nodo Scan csv aplico column pruning leyendo solo 4 de las 17 columnas (MMSI, BaseDateTime, LAT, LON), reduciendo la I/O desde el origen. El shuffle por `hashpartitioning(MMSI)` permite ordenar localmente los datos por tiempo para procesar el `lag()` con un marco de memoria pequeño, mientras que el nodo `PhotonTopK` filtra los 10 mejores registros en cada ejecutor antes de enviar datos por la red a SinglePartition. No se hizo con una UDF de Python ni trayendo todos los datos ordenados al driver porque habria generado una sobrecarga masiva de serializacion entre la JVM y Python, ademas de un cuello de botella de red al transferir millones de agregaciones intermedias sin filtrar previo al shuffle final.

# COMMAND ----------

# MAGIC %md
# MAGIC #### d. ¿Dónde se concentra el tráfico? Top 10 celdas de una grilla espacial (H3 resolución 8, o su propia rejilla lat/lon) por número de posiciones, y cuáles de esas celdas corresponden a puertos del World Port Index.

# COMMAND ----------

# 1. Map each position to its H3 Resolution 8 cell ID
df_h3 = df_ais.withColumn(
    "h3_cell", 
    F.expr("h3_longlatash3string(LON, LAT, 8)")
)

# 2. Find Top 10 spatial cells by position count
top_10_cells = (
    df_h3
    .groupBy("h3_cell")
    .agg(F.count("*").alias("position_count"))
    .orderBy(F.desc("position_count"))
    .limit(10)
)

display(top_10_cells)
top_10_cells.explain("formatted")

# COMMAND ----------

# Leer el archivo de puertos del Wordl Port Index
df_wpi = (
    spark.read
    .option("header", "true")
    .csv(WPI_PATH)
    .select(
        F.col("World Port Index Number").cast("double"),
        F.col("Main Port Name"),
        F.col("Country Code"),
        F.col("Latitude").cast("double"),
        F.col("Longitude").cast("double")
    )
)
display(df_wpi.limit(10))

# COMMAND ----------

# Calcular valor H3 para cada puerto
df_wpi_h3 = df_wpi.withColumn(
    "h3_cell",
    F.expr("h3_longlatash3string(Longitude, Latitude, 8)")
)

# Join Top 10 celdas AIS con World Port Index
top_10_ports_matched = (
    top_10_cells
    .join(df_wpi_h3, on="h3_cell", how="left")
    .select(
        "h3_cell",
        "position_count",
        F.col("Main Port Name").alias("matched_port_name"),
        F.col("Country Code").alias("country")
    )
    .orderBy(F.desc("position_count"))
)

display(top_10_ports_matched)
top_10_ports_matched.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Se encontro que para las posiciones mas transitadas no hay ningun puerto. El enlace directo por ID de celda fue demasiado estricto a esta resolución.
# MAGIC
# MAGIC Esto podria explicarse en primer lugar porque la coordenada del puerto en el World Port Index es un punto arbitrario en tierra o sobre el muelle. Al asignarle una celda, esta no suele coincidir exactamente con la celda sobre el agua donde los barcos transmiten sus señales AIS. Ademas, a resolución 8, cada celda cubre solo ~0.73 km^2. Un barco transitando en la entrada del puerto estará a solo unos cientos de metros de la coordenada oficial, pero en una celda vecina.
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Justificación (d)
# MAGIC
# MAGIC La consulta se hizo de esta manera porque el conteo de posiciones por zona es una **agregación por
# MAGIC clave** (`groupBy(h3_cell).count()`) sobre la que solo se necesita el **Top 10**: el plan resuelve
# MAGIC esto con una agregación parcial en cada ejecutor, un único `hashpartitioning(h3_cell)` y un `TopK`
# MAGIC que recorta a 10 filas *antes* del `shuffle` final, así que por la red viajan decenas de filas y no
# MAGIC millones. Se eligió la grilla **H3 resolución 8** en vez de redondear lat/lon porque H3 da celdas de
# MAGIC área casi constante (~0.73 km²) y vecindad regular, apropiadas para "sectores marítimos" comparables
# MAGIC entre sí; el mapeo `h3_longlatash3string(LON, LAT, 8)` es una expresión nativa (sin UDF de Python),
# MAGIC por lo que se ejecuta dentro de Photon sin costo de serialización JVM↔Python.
# MAGIC
# MAGIC El cruce con el World Port Index se hizo con un **`join` por igualdad de `h3_cell`** (no un
# MAGIC producto cartesiano de distancias) porque comparar cada celda contra cada puerto sería O(celdas ×
# MAGIC puertos); al llevar ambos lados a la misma clave H3, el `join` es una simple igualdad y, como el WPI
# MAGIC es una tabla pequeña, Spark puede difundirla (*broadcast*) y evitar un `shuffle` del lado grande.
# MAGIC
# MAGIC **Resultado y su lectura:** el `join` exacto devolvió `null` para todas las celdas más transitadas.
# MAGIC No es un error del código sino una consecuencia de la resolución: a res 8 cada celda mide ~0.73 km²,
# MAGIC y la coordenada "oficial" de un puerto en el WPI es un punto en el muelle o en tierra, que cae en una
# MAGIC celda distinta a la que ocupan los barcos fondeados/transitando a unos cientos de metros. Es decir,
# MAGIC el emparejamiento por **ID de celda idéntico** es demasiado estricto para este propósito. No se
# MAGIC resolvió aquí con una búsqueda por distancia (vecindad `h3_grid_disk` o *range join* por radio)
# MAGIC porque eso corresponde al enriquecimiento de la Entrega 2; para la Entrega 1 basta con **cuantificar
# MAGIC la concentración de tráfico** (que sí se obtuvo) y documentar por qué el match directo no aplica.

# COMMAND ----------

# MAGIC %md
# MAGIC #### e. ¿Qué proporción de los buques de la semana transmitió los 7 días? ¿Dónde están los "visitantes de un solo día"?

# COMMAND ----------

vessel_days = (
    df_ais
    .withColumn("date", F.to_date("BaseDateTime"))
    .groupBy("MMSI")
    .agg(F.countDistinct("date").alias("days_active"))
)

metrics = vessel_days.select(
    F.count("*").alias("total_unique_vessels"),
    F.sum(F.when(F.col("days_active") == 7, 1).otherwise(0)).alias("vessels_7_days"),
    F.sum(F.when(F.col("days_active") == 1, 1).otherwise(0)).alias("vessels_1_day")
).collect()[0]

total_vessels = metrics["total_unique_vessels"]
count_7_days = metrics["vessels_7_days"]
count_1_day = metrics["vessels_1_day"]

prop_7_days = (count_7_days / total_vessels) * 100
prop_1_day = (count_1_day / total_vessels) * 100

print(f"Total Unique Vessels: {total_vessels:,}")
print(f"Vessels active all 7 days: {count_7_days:,} ({prop_7_days:.2f}%)")
print(f"Single-day visitors (1 day only): {count_1_day:,} ({prop_1_day:.2f}%)")

vessel_days.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC El 39.74% del total de la flota, emitio señales durante los siete dias. El 18.81%, fueron visitantes de un solo dia.

# COMMAND ----------

# Filtrar MMSIs de un solo dia
single_day_mmsis = vessel_days.filter(F.col("days_active") == 1).select("MMSI")

# Mapear visitantes de un solo dia con su ubicacion espacial (H3 resolucion 8)
single_day_locations = (
    df_ais
    .join(single_day_mmsis, on="MMSI", how="inner")
    .withColumn("h3_cell", F.expr("h3_longlatash3string(LON, LAT, 8)"))
    .groupBy("h3_cell")
    .agg(
        F.count("*").alias("position_count"),
        F.countDistinct("MMSI").alias("unique_single_day_vessels"),
    )
    .orderBy(F.desc("unique_single_day_vessels"))
)

display(single_day_locations.limit(10))
single_day_locations.explain("formatted")

# COMMAND ----------

# MAGIC %md
# MAGIC Respecto a la ubicacion espacial de los visitantes de un solo dia, las celdas revelan que la mayor concentracion se registra en la celda `882aa802cdfffff` con 49 embarcaciones distintas, seguida por la celda `8828d54715fffff` que registra 47 buques unicos.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Justificación (e)
# MAGIC
# MAGIC **Proporción de buques por días activos.** Se calculó con `groupBy(MMSI).agg(countDistinct(date))`
# MAGIC para obtener los días distintos en que emitió cada buque, y luego una sola pasada de agregación
# MAGIC condicional (`sum(when(days_active == 7))` y `== 1`) para contar los dos extremos. El plan muestra la
# MAGIC doble agregación por `MMSI` con un `hashpartitioning(MMSI)`: primero se distinguen los pares
# MAGIC `(MMSI, date)` y después se cuentan por buque, todo en Spark nativo. No se usó una `Window` con
# MAGIC `collect_set` de fechas porque materializar el conjunto de fechas por buque es más caro en memoria que
# MAGIC un `countDistinct`, y aquí solo interesa el **número** de días, no el conjunto.
# MAGIC
# MAGIC **Dónde están los visitantes de un solo día.** Se aisló la lista de MMSI con `days_active == 1` y se
# MAGIC volvió a unir contra `df_ais` (`join ... how="inner"`) para geolocalizar solo esas emisiones y
# MAGIC agregarlas por celda H3. La decisión clave es el **tipo de `join`**: la lista de MMSI de un solo día
# MAGIC es pequeña (unos ~6.000 identificadores), así que Spark puede **difundirla (*broadcast join*)** a cada
# MAGIC ejecutor y filtrar el DataFrame grande sin un `shuffle` de los 60 M de filas. Esto equivale a un
# MAGIC "semi-join" barato: se descarta de entrada la gran mayoría de posiciones (las de buques con más de un
# MAGIC día) y solo las restantes llegan al `groupBy(h3_cell)`. No se hizo con una subconsulta correlacionada
# MAGIC ni con un `filter` por `isin()` de miles de valores porque el `broadcast join` por clave es el patrón
# MAGIC que Spark optimiza para "filtrar una tabla grande con una lista pequeña".
# MAGIC
# MAGIC > Verificación opcional: al añadir `.explain("formatted")` a `single_day_locations`, el nodo de
# MAGIC > `join` debería aparecer como *broadcast* (BroadcastHashJoin / PhotonBroadcastHashJoin); si por
# MAGIC > tamaño no se difundiera automáticamente, puede forzarse con `F.broadcast(single_day_mmsis)`.

# COMMAND ----------

# MAGIC %md
# MAGIC # Requisito 4 Almacenamiento óptimo para un propósito

# COMMAND ----------

# MAGIC %md
# MAGIC ## Propósito de consulta y decisiones de almacenamiento
# MAGIC
# MAGIC **Propósito de consulta:** monitoreo diario del operador portuario, que
# MAGIC consulta todas las posiciones de un **día específico** (`date`) dentro de un **sector marítimo**
# MAGIC delimitado por una celda de la grilla espacial (`h3_cell`, resolución 8).
# MAGIC
# MAGIC **Decisiones (se justifican con la evidencia de los pasos B–F):**
# MAGIC
# MAGIC | Decisión | Elección | Por qué |
# MAGIC |---|---|---|
# MAGIC | **Formato de archivo** | **Parquet** sobre CSV | CSV es texto sin tipos ni compresión. Para leer 4 columnas hay que leer la fila completa. Parquet guarda por columna con estadísticas min/max por bloque, habilitando *column pruning* y *predicate pushdown*. |
# MAGIC | **Formato de tabla** | **Delta** sobre solo Parquet | Delta = Parquet + un *transaction log*. El log guarda estadísticas por archivo y permite **data skipping**, `OPTIMIZE` (compactación) y `ZORDER` (co-localización). Con solo Parquet no hay log, así que no puede saltarse archivos por estadísticas ni compactar de forma transaccional. |
# MAGIC | **Layout: particionamiento** | `partitionBy("date")` | El propósito siempre filtra por fecha. Particionar por `date` crea 7 carpetas físicas. Un filtro por fecha lee solo 1 una carpeta (*partition pruning*), descartando 6/7 de los datos sin abrirlos. |
# MAGIC | **Layout: co-localización** | `OPTIMIZE ... ZORDER BY (h3_cell)` | Dentro de cada partición de fecha, el `ZORDER` agrupa las filas de una misma celda espacial en los mismos archivos. Con las estadísticas min/max del log, Delta salta los archivos que no contienen la celda buscada (*data skipping*), la segunda mitad del filtro de la consulta. |
# MAGIC
# MAGIC No se elige CSV como formato final porque no permite *pushdown* ni *skipping* leyendo todo el archivo.<br>
# MAGIC No se elige solo Parquet como tabla porque, sin el log de Delta, no hay estadísticas por archivo para saltar datos, ni `OPTIMIZE`/`ZORDER`. La evidencia de abajo cuantifica ambas cosas.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso A: Preparar columnas de partición (`date`) y grilla espacial (`h3_cell`)
# MAGIC
# MAGIC Sobre el DataFrame de posiciones de los 7 días (`df_ais`) derivamos las dos columnas que usa la consulta

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, countDistinct

# AIS_DIR, PARQUET_PATH y DELTA_TABLE vienen de la celda de sesion.
CSV_PATH = f"{AIS_DIR}/"

# date (particion) + h3_cell (sector espacial del proposito)
df_prepared = (
    df_ais
    .withColumn("date", to_date(col("BaseDateTime")))
    .withColumn("h3_cell", F.expr("h3_longlatash3string(LON, LAT, 8)"))
)

display(df_prepared.select("MMSI", "BaseDateTime", "date", "h3_cell").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso B: Escribir las dos alternativas: Parquet y Delta
# MAGIC
# MAGIC Escribimos el mismo DataFrame en los dos formatos que vamos a comparar, ambos particionados por `date`
# MAGIC para que la comparación sea justa. Mismo layout lógico, distinto motor de tabla.

# COMMAND ----------

# Baseline en Parquet (sin transaction log)
(df_prepared.write
    .format("parquet")
    .mode("overwrite")
    .partitionBy("date")
    .save(PARQUET_PATH)
)

# Tabla Delta (Parquet + transaction log) particionada por fecha
(df_prepared.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .option("overwriteSchema", "true")
    .saveAsTable(DELTA_TABLE)
)

print("Parquet escrito en:", PARQUET_PATH)
print("Tabla Delta creada:", DELTA_TABLE)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso C: Evidencia 1: bytes antes / después (CSV, Parquet, Delta)
# MAGIC
# MAGIC Medimos el tamaño de cada representación. Para CSV y Parquet recorremos el directorio con
# MAGIC la Files API (`w.files`). Para Delta leemos el tamaño autoritativo del *transaction log* con `DESCRIBE DETAIL`.

# COMMAND ----------

def dir_size_bytes(path):
    """Suma recursiva del tamano de todos los archivos bajo un path del Volume.

    La Files API sustituye a dbutils.fs.ls: con Databricks Connect, dbutils no existe en el laptop.
    """
    total = 0
    for entry in w.files.list_directory_contents(path.rstrip("/")):
        if entry.is_directory:
            total += dir_size_bytes(entry.path)
        else:
            total += entry.file_size or 0
    return total


def gb(b):
    return b / (1024 ** 3)

# CSV y Parquet: tamano en disco por recorrido de directorio
size_csv = dir_size_bytes(CSV_PATH)
size_parquet = dir_size_bytes(PARQUET_PATH)

# Delta: tamano desde el log (sizeInBytes) + numFiles
detail = spark.sql(f"DESCRIBE DETAIL {DELTA_TABLE}").select("sizeInBytes", "numFiles").collect()[0]
size_delta = detail["sizeInBytes"]
delta_num_files = detail["numFiles"]

print(f"CSV     : {gb(size_csv):6.2f} GB")
print(f"Parquet : {gb(size_parquet):6.2f} GB   ({(1 - size_parquet/size_csv)*100:5.1f}% menos que CSV)")
print(f"Delta   : {gb(size_delta):6.2f} GB   ({(1 - size_delta/size_csv)*100:5.1f}% menos que CSV)  | archivos: {delta_num_files}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso D: Evidencia 2: archivos escaneados por la consulta
# MAGIC
# MAGIC Ejecutamos la consulta sobre cada fuente y recogemos **dos evidencias complementarias**:
# MAGIC
# MAGIC 1. **Plan de ejecución** (`explain("formatted")`): en el nodo `Scan` se leen los
# MAGIC    `PartitionFilters` (prueba del *partition pruning* por `date`) y las `DictionaryFilters` /
# MAGIC    `RequiredDataFilters` sobre `h3_cell` (el *data skipping* que Delta aplica con las estadísticas de su *transaction log*).
# MAGIC
# MAGIC 2. **Archivos con datos** (`_metadata.file_path`): número de archivos físicos que aportan filas al
# MAGIC    resultado. Menos archivos = mejor *pruning/skipping* para este patrón de acceso.
# MAGIC    
# MAGIC    (Nota: en Unity Catalog `input_file_name()` no está soportado. Se usa la columna `_metadata.file_path`.)
# MAGIC
# MAGIC Ambos formatos aplican el mismo *partition pruning* por `date` (idéntico `PartitionFilters` en el
# MAGIC plan), así que descartan 6 de los 7 días. La diferencia aparece **dentro** del día: Parquet debe
# MAGIC abrir todos los *part-files* de esa partición, mientras que Delta usa las estadísticas por archivo
# MAGIC de su log para saltar los que no contienen la celda buscada.

# COMMAND ----------

target_date = "2023-06-01"
target_cell = "882aa802cdfffff" # celda con mas visitantes de un solo dia (Req 3e)

def purpose_query(df):
    """La consulta especificada: filtra por fecha (particion) y sector espacial (h3_cell)."""
    return df.filter((col("date") == target_date) & (col("h3_cell") == target_cell))

def files_and_rows(df, label, show_plan=False):
    q = purpose_query(df)
    n_files = q.withColumn("_file", col("_metadata.file_path")).select(countDistinct("_file")).collect()[0][0]
    n_rows  = q.count()
    print(f"{label:26s}: Archivos con datos: {n_files:4d} . Filas: {n_rows}")
    if show_plan:
        print(f"\n----- PLAN: {label} -----")
        q.explain("formatted")
        print()
    return n_files, n_rows

df_parquet = spark.read.format("parquet").load(PARQUET_PATH)
df_delta = spark.read.table(DELTA_TABLE)

print("=== Archivos escaneados por la consulta ===")
f_par, _ = files_and_rows(df_parquet, "Parquet (partition date)",   show_plan=True)
f_del_pre, _ = files_and_rows(df_delta,   "Delta ANTES de OPTIMIZE", show_plan=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Paso E: Aplicar `OPTIMIZE` y `ZORDER BY (h3_cell)`
# MAGIC
# MAGIC `OPTIMIZE` reescribe los datos de cada partición y `ZORDER BY (h3_cell)` co-localiza en los mismos
# MAGIC archivos las filas de una misma celda espacial, para maximizar el *data skipping* de consultas por
# MAGIC zona. La fila de métricas de salida (`numFilesAdded` / `numFilesRemoved`) documenta la reescritura.
# MAGIC
# MAGIC **Qué esperar con nuestros datos.** Nuestra tabla es pequeña por partición (7 archivos, uno por día).
# MAGIC `OPTIMIZE` no reduce el número de archivos aquí: al aplicar `ZORDER` la tabla pasa de 7 a 21
# MAGIC archivos, porque el reordenamiento por celda divide cada archivo diario en varios *Z-cubes*. La
# MAGIC compactación que baja el conteo de archivos solo ayuda cuando hay muchos archivos pequeños por
# MAGIC partición que no es nuestro caso. Por eso medimos el efecto sobre la **consulta**, no solo sobre el conteo de archivos.

# COMMAND ----------

display(spark.sql(f"""
-- Compactacion y co-localizacion espacial dentro de cada particion de fecha
OPTIMIZE {DELTA_TABLE}
ZORDER BY (h3_cell)
"""))

# COMMAND ----------

# Volver a medir tras OPTIMIZE/ZORDER
df_delta_opt = spark.read.table(DELTA_TABLE)

print("=== Archivos escaneados: efecto de OPTIMIZE + ZORDER ===")
f_del_post, _ = files_and_rows(df_delta_opt, "Delta DESPUES de OPTIMIZE", show_plan=True)

detail2 = spark.sql(f"DESCRIBE DETAIL {DELTA_TABLE}").select("numFiles", "sizeInBytes").collect()[0]
print()
print(f"Delta numFiles: {delta_num_files} (antes)  ->  {detail2['numFiles']} (despues de compactar)")
print("Archivos tocados por la consulta del proposito:")
print(f"  Parquet                 : {f_par}")
print(f"  Delta (antes OPTIMIZE)  : {f_del_pre}")
print(f"  Delta (despues ZORDER)  : {f_del_post}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumen comparativo (formato, bytes y archivos leídos)
# MAGIC
# MAGIC Tabla única con las tres evidencias juntas para la consulta
# MAGIC (`date = 2023-06-01 AND h3_cell = 882aa802cdfffff`).

# COMMAND ----------

# Resumen: consolida bytes (Paso C) y archivos leidos por la consulta (Pasos D y E)
resumen = spark.createDataFrame(
    [
        ("CSV (crudo)", round(gb(size_csv), 2), None, None),
        ("Parquet (partition date)", round(gb(size_parquet), 2), int(f_par), None),
        ("Delta (antes OPTIMIZE)", round(gb(size_delta), 2), int(f_del_pre), int(delta_num_files)),
        ("Delta (despues ZORDER)", round(gb(size_delta), 2), int(f_del_post), int(detail2["numFiles"])),
    ],
    ["formato", "tamano_GB", "archivos_leidos_consulta", "archivos_totales_tabla"],
)

display(resumen)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusión
# MAGIC
# MAGIC El almacenamiento se resolvió con Delta particionado por `date` porque el propósito de consulta
# MAGIC filtra siempre por fecha y por sector espacial, y el layout ataca esas dos dimensiones. La evidencia
# MAGIC medida lo respalda:
# MAGIC
# MAGIC 1. **Bytes (Paso C):** la compresión columnar reduce el tamaño de forma drástica frente al CSV crudo:
# MAGIC    **CSV 6.04 GB -> Parquet 2.01 GB (−66.7%) -> Delta 1.48 GB (−75.6%)**.
# MAGIC 2. **Archivos leídos por la consulta (Paso D):** con el mismo `PartitionFilters` por
# MAGIC    `date`, Parquet abre 6 archivos de la partición del día, mientras que Delta abre solo 1.
# MAGIC    Esa diferencia es el *data skipping* de Delta: las estadísticas por archivo de su
# MAGIC    *transaction log*, combinadas con las `DictionaryFilters`/`RequiredDataFilters` sobre `h3_cell`
# MAGIC    del plan, le permiten saltar los archivos que no contienen la celda buscada. Es la ganancia de
# MAGIC    Delta sobre Parquet, y ocurre sin necesidad de `OPTIMIZE`.
# MAGIC 3. **Efecto de `OPTIMIZE` + `ZORDER` (Paso E):** a nuestra escala **no mejora la consulta** (Delta ya
# MAGIC    leía 1 solo archivo por partición) y de hecho sube el número de archivos de 7 a 21 por el
# MAGIC    reordenamiento en *Z-cubes*. `OPTIMIZE` es una herramienta para tablas con muchos archivos
# MAGIC    pequeños por partición. Con 7 días y un archivo por día no hay nada que compactar, así que su
# MAGIC    beneficio no se nota aqui. Se documenta como parte del análisis, no como una mejora observada.
# MAGIC
# MAGIC **Decisión final:** *Delta particionado por `date`*. `ZORDER`/`OPTIMIZE` se mantienen en el pipeline porque escalarían con más días o escritura incremental, a pesar de que con 7 días su efecto sobre la consulta sea nulo.

# COMMAND ----------

# MAGIC %md
# MAGIC # Requisito 5 Gobernanza: verificacion de comentarios y metadatos
# MAGIC
# MAGIC Los comentarios (`COMMENT`) y metadatos (`TBLPROPERTIES`) sobre el catalogo, el esquema, la tabla,
# MAGIC sus columnas y el volumen se aplicaron en Unity Catalog. Como esos objetos viven en el metastore y no
# MAGIC en el repositorio Git, esta consulta deja evidencia en el notebook de que la gobernanza
# MAGIC esta aplicada, sin acceso directo al workspace.
# MAGIC
# MAGIC `DESCRIBE EXTENDED` muestra el comentario de la tabla y sus `Table Properties`.

# COMMAND ----------

display(spark.sql(f"""
-- Comentario de tabla + propiedades (buscar 'Comment' y 'Table Properties' en la salida)
DESCRIBE EXTENDED {DELTA_TABLE}
"""))

import json, uuid, io

NB = "notebook/proyecto_ocean_watch.ipynb"
with io.open(NB, "r", encoding="utf-8") as f:
    nb = json.load(f)
cells = nb["cells"]

# Anchor checks: cell 13 = quality_diagnostics code, cell 14 = Hallazgos markdown
assert "quality_diagnostics = df_flagged.select" in "".join(cells[13]["source"]), "cell 13 no es el codigo del Paso 4"
assert "".join(cells[14]["source"]).startswith("## Diagnostico y hallazgos"), "cell 14 no es Hallazgos"


def nuid():
    return str(uuid.uuid4())


def md(text):
    return {
        "cell_type": "markdown",
        "metadata": {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {}, "inputWidgets": {}, "nuid": nuid(),
            "showTitle": False, "tableResultSettingsMap": {}, "title": ""}},
        "source": text.split("\n"),
    }


def code(text):
    return {
        "cell_type": "code", "execution_count": 0,
        "metadata": {"application/vnd.databricks.v1+cell": {
            "cellMetadata": {"byteLimit": 2048000, "rowLimit": 10000},
            "inputWidgets": {}, "nuid": nuid(), "showTitle": False,
            "tableResultSettingsMap": {}, "title": ""}},
        "outputs": [], "source": text.split("\n"),
    }


new_cells = []

# ---- Paso 5: Auditoria de nulos / completitud ----
new_cells.append(md(
    "## Paso 5: Auditoria de nulos (completitud por columna)\n"
    "\n"
    "Antes de la Entrega 2 hay que saber **en que columnas se puede confiar**. Contamos, en una sola\n"
    "pasada, cuantos valores nulos tiene cada una de las 17 columnas y su porcentaje sobre el total.\n"
    "Columnas con muchos nulos (p. ej. `IMO`, `Cargo`, `Draft`) no sirven como llaves ni como features\n"
    "sin imputacion; columnas casi completas (`MMSI`, `BaseDateTime`, `LAT`, `LON`) son la base confiable."
))
new_cells.append(code(
    "# Conteo de nulos por columna, en una sola pasada sobre los 60 M de filas.\n"
    "# Se cuenta como nulo el valor NULL y, en columnas numericas, tambien NaN.\n"
    "cols = df_ais.columns\n"
    "\n"
    "null_exprs = [\n"
    "    F.sum(F.when(F.col(c).isNull(), 1).otherwise(0)).alias(c)\n"
    "    for c in cols\n"
    "]\n"
    "null_row = df_ais.select(null_exprs).collect()[0]\n"
    "total = df_ais.count()\n"
    "\n"
    "null_summary = spark.createDataFrame(\n"
    "    [(c, int(null_row[c]), (null_row[c] / total) * 100) for c in cols],\n"
    "    [\"Columna\", \"Nulos\", \"Porcentaje_Nulos\"]\n"
    ").orderBy(F.desc(\"Nulos\"))\n"
    "\n"
    "display(null_summary)"
))

# ---- Paso 6: Consistencia de atributos estaticos por MMSI ----
new_cells.append(md(
    "## Paso 6: Consistencia de atributos estaticos por buque (MMSI)\n"
    "\n"
    "Un mismo buque (`MMSI`) deberia reportar **siempre** el mismo tipo y las mismas dimensiones durante la\n"
    "semana, porque son atributos estaticos del casco. Cuando un `MMSI` reporta **mas de un `VesselType`** o\n"
    "**mas de una eslora/manga**, hay una inconsistencia: reprogramacion del transpondedor, reutilizacion de\n"
    "MMSI entre barcos, o error de captura. Es un hallazgo clave para la Entrega 2, porque decide si podemos\n"
    "confiar en los atributos estaticos o si hay que resolver conflictos (p. ej. tomar el valor mas frecuente\n"
    "por MMSI). Contamos cuantos MMSI presentan cada tipo de inconsistencia."
))
new_cells.append(code(
    "# Para cada MMSI, cuantos valores DISTINTOS toma cada atributo estatico\n"
    "mmsi_consistency = (\n"
    "    df_ais\n"
    "    .groupBy(\"MMSI\")\n"
    "    .agg(\n"
    "        F.countDistinct(\"VesselType\").alias(\"distinct_types\"),\n"
    "        F.countDistinct(\"Length\").alias(\"distinct_lengths\"),\n"
    "        F.countDistinct(\"Width\").alias(\"distinct_widths\"),\n"
    "        F.countDistinct(\"VesselName\").alias(\"distinct_names\")\n"
    "    )\n"
    ")\n"
    "\n"
    "resumen_consistencia = mmsi_consistency.select(\n"
    "    F.count(\"*\").alias(\"total_mmsi\"),\n"
    "    F.sum(F.when(F.col(\"distinct_types\") > 1, 1).otherwise(0)).alias(\"mmsi_varios_tipos\"),\n"
    "    F.sum(F.when(F.col(\"distinct_lengths\") > 1, 1).otherwise(0)).alias(\"mmsi_varias_esloras\"),\n"
    "    F.sum(F.when(F.col(\"distinct_widths\") > 1, 1).otherwise(0)).alias(\"mmsi_varias_mangas\"),\n"
    "    F.sum(F.when(F.col(\"distinct_names\") > 1, 1).otherwise(0)).alias(\"mmsi_varios_nombres\")\n"
    ").collect()[0]\n"
    "\n"
    "tm = resumen_consistencia[\"total_mmsi\"]\n"
    "\n"
    "def pct(n):\n"
    "    return (n / tm) * 100\n"
    "\n"
    "consistency_summary = spark.createDataFrame([\n"
    "    (\"MMSI con mas de un VesselType\", resumen_consistencia[\"mmsi_varios_tipos\"], pct(resumen_consistencia[\"mmsi_varios_tipos\"])),\n"
    "    (\"MMSI con mas de una eslora (Length)\", resumen_consistencia[\"mmsi_varias_esloras\"], pct(resumen_consistencia[\"mmsi_varias_esloras\"])),\n"
    "    (\"MMSI con mas de una manga (Width)\", resumen_consistencia[\"mmsi_varias_mangas\"], pct(resumen_consistencia[\"mmsi_varias_mangas\"])),\n"
    "    (\"MMSI con mas de un VesselName\", resumen_consistencia[\"mmsi_varios_nombres\"], pct(resumen_consistencia[\"mmsi_varios_nombres\"])),\n"
    "], [\"Inconsistencia\", \"MMSI_Afectados\", \"Porcentaje_De_MMSI\"])\n"
    "\n"
    "print(f\"Total de MMSI distintos: {tm:,}\")\n"
    "display(consistency_summary)\n"
    "\n"
    "# Ejemplos concretos: los MMSI que mas tipos distintos reportan\n"
    "display(\n"
    "    mmsi_consistency\n"
    "    .filter(F.col(\"distinct_types\") > 1)\n"
    "    .orderBy(F.desc(\"distinct_types\"))\n"
    "    .limit(10)\n"
    ")"
))

# ---- Paso 7: Anomalias adicionales (temporal + VesselType invalido) ----
new_cells.append(md(
    "## Paso 7: Anomalias adicionales (temporales y de catalogo)\n"
    "\n"
    "Dos inconsistencias baratas de medir y valiosas para la Entrega 2:\n"
    "\n"
    "- **Posiciones imposibles en el tiempo**: un mismo `(MMSI, BaseDateTime)` con **coordenadas distintas**\n"
    "  significa que el buque estaria en dos lugares a la vez en el mismo instante. Es un conflicto distinto\n"
    "  del duplicado exacto del Paso 1 (mismo instante, distinto lugar) y hay que resolverlo antes de calcular\n"
    "  trayectorias o distancias.\n"
    "- **`VesselType` fuera de catalogo**: los tipos AIS validos van de 0 a 99. Contamos las filas con\n"
    "  `VesselType` nulo o fuera de ese rango (en los datos aparecen codigos como 107, 136, 200)."
))
new_cells.append(code(
    "# 1. Posiciones imposibles en el tiempo: mismo (MMSI, BaseDateTime) con coordenadas distintas\n"
    "time_window = Window.partitionBy(\"MMSI\", \"BaseDateTime\")\n"
    "df_time = df_ais.withColumn(\"distinct_coords\", F.size(F.collect_set(F.concat_ws(\",\", \"LAT\", \"LON\")).over(time_window)))\n"
    "impossible_time = df_time.filter(F.col(\"distinct_coords\") > 1).count()\n"
    "\n"
    "# 2. VesselType fuera del catalogo AIS (valido 0-99) o nulo\n"
    "invalid_type = df_ais.filter(\n"
    "    F.col(\"VesselType\").isNull() | (F.col(\"VesselType\") < 0) | (F.col(\"VesselType\") > 99)\n"
    ").count()\n"
    "\n"
    "# 3. Volumen de filas con MMSI = 0 (no atribuibles a un buque)\n"
    "mmsi_cero = df_ais.filter((F.col(\"MMSI\") == \"0\") | F.col(\"MMSI\").rlike(\"^0+$\")).count()\n"
    "\n"
    "total = df_ais.count()\n"
    "anomalias = spark.createDataFrame([\n"
    "    (\"Posiciones imposibles en el tiempo (mismo MMSI+instante, distinto lugar)\", impossible_time, (impossible_time / total) * 100),\n"
    "    (\"VesselType fuera de catalogo (nulo o fuera de 0-99)\", invalid_type, (invalid_type / total) * 100),\n"
    "    (\"MMSI = 0 (no atribuible a un buque)\", mmsi_cero, (mmsi_cero / total) * 100),\n"
    "], [\"Anomalia\", \"Filas_Afectadas\", \"Porcentaje_Del_Total\"])\n"
    "\n"
    "display(anomalias)"
))

# Splice after cell 13 (Paso 4 code), before cell 14 (Hallazgos)
cells[:] = cells[:14] + new_cells + cells[14:]
nb["cells"] = cells

# accent/emoji guard for the cells we just added
import re
bad = []
for c in new_cells:
    if c["cell_type"] == "code":
        for line in c["source"]:
            for ch in line:
                if ch in "áéíóúüñÁÉÍÓÚÜÑ¿¡":
                    bad.append(repr(ch))
                if ord(ch) > 0x1F000:
                    bad.append("emoji:" + repr(ch))

with io.open(NB, "w", encoding="utf-8") as f:
    json.dump(nb, f, ensure_ascii=False, indent=1)

print("Insertadas", len(new_cells), "celdas. Total:", len(cells))
print("acentos/emojis en codigo nuevo:", bad if bad else "NONE")

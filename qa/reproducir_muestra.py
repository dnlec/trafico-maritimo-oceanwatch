"""Reproduce los CSV de validación en Spark y un registro MLflow local, desde los ZIP originales.

python reproducir_muestra.py --zips CARPETA_ZIP --trabajo CARPETA_NUEVA [--jars CARPETA_JARS]
Para repetir la tolerancia a fallos en Databricks, usar prueba_recuperacion=si en 03_silver.
"""
import argparse
import datetime as dt
import json
import os
import shutil
import sys
from pathlib import Path
from zipfile import ZipFile

import duckdb
import h3
import mlflow
import pandas as pd
from mlflow import MlflowClient
from pyspark.sql import SparkSession, Window, functions as F

RAIZ = Path(__file__).resolve().parents[1]
DIAS = ["2023-06-07","2023-06-08","2023-06-09"]


def celda(notebook, fragmento):
    libro = json.loads((RAIZ/"src"/(notebook+".ipynb")).read_text())
    textos = ["".join(c["source"]) for c in libro["cells"] if c["cell_type"]=="code"]
    return next(s for s in textos if fragmento in s)


def preparar(zips, trabajo):
    c = duckdb.connect()
    c.execute("SET threads=3")
    resumen = {}
    for d in DIAS:
        ruta=zips/("AIS_"+d.replace("-","_")+".zip")
        plano=trabajo/(d+".csv")
        with ZipFile(ruta) as zipf:
            miembros=[k for k in zipf.namelist() if k.lower().endswith(".csv") and "__MACOSX" not in k]
            if len(miembros)!=1:
                raise ValueError(f"Se esperaba un CSV en {ruta}.")
            with zipf.open(miembros[0]) as entrada, plano.open("wb") as salida:
                shutil.copyfileobj(entrada,salida,4*1024*1024)
        fuente=c.read_csv(str(plano),header=True,all_varchar=True)
        archivo=trabajo/(d+".parquet")
        fuente.filter("substr(md5(coalesce(MMSI,'')),1,2) < '08'").write_parquet(str(archivo))
        resumen[d]={"total_csv":fuente.count("*").fetchone()[0],
                    "muestra":c.read_parquet(str(archivo)).count("*").fetchone()[0]}
        plano.unlink()
    c.close()
    return resumen


def main(args):
    global spark, GOLD_TRAY, GOLD_ANOM, CONTROL_ORO, CONTROL_ML, MODELO, fecha
    trabajo=args.trabajo.resolve()
    if trabajo.exists() and any(trabajo.iterdir()):
        raise ValueError("Usa una carpeta de trabajo nueva para aislar el registro de esta prueba.")
    trabajo.mkdir(parents=True,exist_ok=True)
    salidas=trabajo/"salidas";salidas.mkdir()
    conteos=preparar(args.zips,trabajo)
    (salidas/"conteos_fuente.json").write_text(json.dumps(conteos,indent=2))
    os.environ.setdefault("SPARK_LOCAL_IP","127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON",sys.executable)
    builder=(SparkSession.builder.master("local[2]").appName("OceanWatch · validación NOAA")
        .config("spark.sql.extensions","io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog","org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.warehouse.dir",str(trabajo/"warehouse"))
        .config("spark.sql.shuffle.partitions",8).config("spark.databricks.delta.snapshotPartitions",2)
        .config("spark.driver.memory","3g").config("spark.sql.session.timeZone","UTC"))
    if args.jars:
        jars=sorted(args.jars.resolve().glob("*.jar"))
        if not jars:raise ValueError("Faltan los JAR de Delta y delta-storage.")
        builder=builder.config("spark.jars",",".join(str(p) for p in jars))
    else:
        from delta import configure_spark_with_delta_pip
        builder=configure_spark_with_delta_pip(builder)
    spark=builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    # Las funciones se cargan del código de la entrega para probar la misma implementación.
    exec(celda("03_silver","def con_motivo("),globals())
    exec(celda("04_gold","def trayectorias("),globals())
    exec(celda("04_gold","def trafico_diario("),globals())
    exec("def presencia_diaria"+celda("04_gold","def presencia_diaria(").split("def presencia_diaria",1)[1],globals())
    exec(celda("05_oceanwatch","def matriz("),globals())
    exec(celda("05_oceanwatch","def atributos("),globals())
    esquema=celda("02_bronze","ais_schema =").split('ais_schema = """')[1].split('"""')[0]
    raw=(spark.read.parquet(str(trabajo/"*.parquet")).select(*[
        F.expr(f"try_cast(`{n}` as {tipo})").alias(n) for n,tipo in [x.split() for x in esquema.split(",")]])
        .withColumn("_source_file",F.date_format("BaseDateTime","yyyy-MM-dd"))
        .withColumn("_ingested_at",F.lit("2023-06-10").cast("timestamp")))
    mask=(spark.read.schema("tesela INT,clase INT,bits_hex STRING").option("header",True)
          .csv(str(RAIZ/"reference/agua_ne_v1.csv.gz")))
    def materializar(df,n):
        destino=trabajo/n
        df.write.parquet(str(destino))
        nuevo=spark.read.parquet(str(destino))
        nuevo.createOrReplaceTempView(n)
        return nuevo
    marcado=materializar(con_motivo(raw,mask),"marcado")
    orden=Window.partitionBy("MMSI","BaseDateTime").orderBy("_source_file",F.to_json(F.struct(*COLUMNAS[:-2])),"_ingested_at")
    plata=materializar(marcado.where("motivo=''").withColumn("rn",F.row_number().over(orden)).where("rn=1")
         .drop("rn","motivo").withColumn("fecha",F.to_date("BaseDateTime")),"silver")
    spark.udf.register("h3_longlatash3string",lambda lon,lat,res:h3.latlng_to_cell(lat,lon,res),"string")
    def centro(celda_h3):
        lat,lon=h3.cell_to_latlng(celda_h3)
        return json.dumps({"type":"Point","coordinates":[lon,lat]})
    spark.udf.register("h3_centerasgeojson",centro,"string")
    campos=["Main Port Name","Country Code","Latitude","Longitude","World Port Index Number"]
    wpi=pd.read_csv(RAIZ/"UpdatedPub150.csv",usecols=campos)
    vecinos=[(int(n),str(nombre),str(pais),c) for nombre,pais,lat,lon,n in wpi[campos].itertuples(index=False,name=None)
             for c in h3.grid_disk(h3.latlng_to_cell(lat,lon,7),1)]
    puertos=spark.createDataFrame(vecinos,"numero_puerto int,nombre string,pais string,h3_res7 string").dropDuplicates(["numero_puerto","h3_res7"])
    materializar(trayectorias(plata),"tray");materializar(presencia_diaria(plata,F.broadcast(puertos)),"puertos")
    materializar(trafico_diario(plata),"trafico")
    GOLD_TRAY,GOLD_ANOM,CONTROL_ORO,CONTROL_ML="tray","qa_anomalias","fechas_oro","qa_control_ml"
    MODELO,fecha="oceanwatch.qa.detector_velocidad",DIAS[-1]
    spark.sql("SELECT fecha,0L lote_plata FROM tray GROUP BY fecha").createOrReplaceTempView(CONTROL_ORO)
    spark.sql(f"CREATE TABLE {CONTROL_ML} (fecha DATE,revision_oro STRING,publicado_en TIMESTAMP) USING DELTA")
    spark.sql(f"""CREATE TABLE {GOLD_ANOM} (fecha DATE,MMSI STRING,n_posiciones BIGINT,sog_media DOUBLE,
       velocidad_tramos DOUBLE,brecha DOUBLE,score DOUBLE,es_anomalia BOOLEAN,estado STRING,
       modelo_version STRING,run_id STRING) USING DELTA PARTITIONED BY (fecha)""")
    uri="sqlite:///"+str(trabajo/"registro.sqlite")
    mlflow.set_tracking_uri(uri);mlflow.set_registry_uri(uri)
    cliente=MlflowClient()
    exp="OceanWatch-QA"
    cliente.create_experiment(exp,artifact_location=(trabajo/"modelos").as_uri())
    ejecutar_ml(uri,exp)
    versiones=cliente.search_model_versions(f"name = '{MODELO}'")
    ejecutar_ml(uri,exp)
    assert len(versiones)==len(cliente.search_model_versions(f"name = '{MODELO}'"))
    def csv(df,n):
        df.toPandas().to_csv(salidas/(n+".csv"),index=False)
    dash=json.loads((RAIZ/"dashboards/OceanWatch_Institucional.lvdash.json").read_text())
    for ds in dash["datasets"]:
        sql="".join(ds["queryLines"])
        for corto,n in zip(["trayectorias","presencia_puertos","trafico_celda_hora","anomalias"],["tray","puertos","trafico",GOLD_ANOM]):
            sql=sql.replace("dbr_acad_ingsistemas_mine.oceanwatch_oro_eq8.oceanwatch_gold_"+corto,n)
        csv(spark.sql(sql,args={"dia":dt.date.fromisoformat(fecha)}),ds["name"])
    csv(spark.sql("SELECT to_date(BaseDateTime) fecha,motivo,count(*) count FROM marcado GROUP BY ALL ORDER BY fecha,motivo"),"motivos")
    csv(spark.sql(f"SELECT fecha,estado,modelo_version,run_id,count(*) buques,count_if(es_anomalia) alertas FROM {GOLD_ANOM} GROUP BY ALL ORDER BY fecha,estado"),"cobertura_ml")
    balance=spark.sql("""SELECT s.fecha,s.posiciones_plata,s.buques,m.trafico,t.trayectorias FROM
        (SELECT fecha,count(*) posiciones_plata,count(distinct MMSI) buques FROM silver GROUP BY fecha) s JOIN
        (SELECT fecha,sum(n_posiciones) trafico FROM trafico GROUP BY fecha) m USING(fecha) JOIN
        (SELECT fecha,sum(n_posiciones) trayectorias FROM tray GROUP BY fecha) t USING(fecha) ORDER BY fecha""")
    csv(balance,"balance")
    for fila in balance.collect(): assert fila.posiciones_plata==fila.trafico==fila.trayectorias
    for tabla,llave in [("tray","fecha,MMSI"),("puertos","fecha,numero_puerto,MMSI"),("trafico","fecha,hora,h3_cell"),(GOLD_ANOM,"fecha,MMSI")]:
        assert not spark.sql(f"SELECT {llave} FROM {tabla} GROUP BY {llave} HAVING count(*)>1").take(1)
    runs=mlflow.search_runs(experiment_names=[exp])
    campos=[k for k in runs if k in ("run_id","status","tags.mlflow.runName","tags.fecha_evaluacion","tags.huella","tags.fuente",
              "tags.modelo_version","tags.run_entrenamiento") or k.startswith(("params.","metrics."))]
    runs[campos].to_csv(salidas/"runs_mlflow_local.csv",index=False)
    spark.stop()
    print("Conciliación, llaves y reejecución correctas.",salidas)


if __name__=="__main__":
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument("--zips",type=Path,required=True)
    p.add_argument("--trabajo",type=Path,required=True)
    p.add_argument("--jars",type=Path)
    main(p.parse_args())

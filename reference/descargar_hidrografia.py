"""Descarga las referencias públicas para reconstruir la máscara del proyecto."""
import argparse
import json
import shutil
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen

CAPAS = ("land", "lakes", "lakes_north_america", "rivers_lake_centerlines", "rivers_north_america")
RED = "https://services7.arcgis.com/n1YM8pTrFmm7L4hs/ArcGIS/rest/services/Waterway_Networks/FeatureServer/1"


def descargar(destino):
    destino.mkdir(parents=True, exist_ok=True)
    urls = [f"https://naturalearth.s3.amazonaws.com/10m_physical/ne_10m_{c}.zip" for c in CAPAS]
    urls += ["https://www2.census.gov/geo/tiger/TIGER2023/COASTLINE/tl_2023_us_coastline.zip"]
    for url in urls:
        with urlopen(url, timeout=120) as fuente, (destino / url.rsplit("/", 1)[1]).open("wb") as f:
            shutil.copyfileobj(fuente, f)
    def consultar(parametros):
        with urlopen(RED+"/query?"+urlencode(parametros), timeout=120) as respuesta:
            return json.load(respuesta)
    with urlopen(RED+"?f=json", timeout=60) as resp:
        oid = json.load(resp)["objectIdField"]
    ids = sorted(consultar({"where":"1=1","returnIdsOnly":"true","f":"json"})["objectIds"])
    def tramo(lote):
        return consultar({"objectIds":",".join(map(str,lote)), "outFields":oid, "returnGeometry":"true",
                          "outSR":4326, "geometryPrecision":6, "f":"geojson"})["features"]
    lotes = [ids[i:i+250] for i in range(0,len(ids),250)]
    with ThreadPoolExecutor(max_workers=4) as pool:
        filas = [row for lote in pool.map(tramo,lotes) for row in lote]
    if len(filas) != len(ids):
        raise ValueError("Respuesta incompleta de la Red Nacional de Vías Navegables.")
    (destino/"nwn_usace.geojson").write_text(json.dumps({"type":"FeatureCollection","features":filas},
        separators=(",",":")),encoding="utf-8")


if __name__=="__main__":
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument("destino",type=Path)
    descargar(parser.parse_args().destino)

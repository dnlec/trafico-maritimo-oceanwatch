"""Reconstruye la referencia de agua con GDAL 3, NumPy y las capas de Natural Earth, US Census y USACE.

Uso: python construir_mascara.py carpeta_con_zips agua_ne_v1.csv.gz
Las descargas y sus huellas están documentadas en el contrato de oro.
"""
import argparse
import csv
import gzip
import io
import tempfile
from pathlib import Path

import numpy as np
from osgeo import gdal, ogr

CAPAS = ("land", "lakes", "lakes_north_america", "rivers_lake_centerlines", "rivers_north_america")
PASOS = 200  # 0,005 grados por píxel


def construir(carpeta, destino):
    gdal.UseExceptions()
    gdal.SetCacheMax(160 * 1024 * 1024)
    with tempfile.TemporaryDirectory() as temp:
        grid = gdal.GetDriverByName("GTiff").Create(
            str(Path(temp) / "tierra.tif"), 360 * PASOS, 180 * PASOS, 1, gdal.GDT_Byte,
            ["TILED=YES", "SPARSE_OK=YES", "COMPRESS=DEFLATE", "BIGTIFF=IF_SAFER"])
        grid.SetGeoTransform((-180, 1 / PASOS, 0, 90, 0, -1 / PASOS))
        for capa in CAPAS:
            nombre = "ne_10m_" + capa
            ruta = carpeta.resolve() / (nombre + ".zip")
            origen = ogr.Open(f"/vsizip/{ruta}/{nombre}.shp")
            if origen is None:
                raise FileNotFoundError(ruta)
            layer = origen.GetLayer()
            grid.SetProjection(layer.GetSpatialRef().ExportToWkt())
            opciones = ["ALL_TOUCHED=TRUE"] if capa.startswith("rivers") else []
            gdal.RasterizeLayer(grid, [1], layer, burn_values=[int(capa == "land")], options=opciones)
        costa = carpeta.resolve() / "tl_2023_us_coastline.zip"
        for path in (f"/vsizip/{costa}/tl_2023_us_coastline.shp", str(carpeta.resolve() / "nwn_usace.geojson")):
            origen = ogr.Open(path)
            if origen is None:
                raise FileNotFoundError(path)
            # La celda contiene un tramo real de costa o de vía navegable.
            gdal.RasterizeLayer(grid, [1], origen.GetLayer(), burn_values=[0], options=["ALL_TOUCHED=TRUE"])
        grid.FlushCache()
        with destino.open("wb") as raw, gzip.GzipFile(filename="", fileobj=raw, mode="wb", mtime=0) as gz:
            with io.TextIOWrapper(gz, encoding="ascii", newline="") as salida:
                w = csv.writer(salida, lineterminator="\n")
                w.writerow(["tesela", "clase", "bits_hex"])
                for y in range(180):
                    franja = grid.ReadAsArray(0, y * PASOS, 360 * PASOS, PASOS)
                    for x in range(360):
                        bloque = franja[:, x * PASOS:(x + 1) * PASOS]
                        if not bloque.any():
                            clase, bits = 0, ""
                        elif bloque.all():
                            clase, bits = 1, ""
                        else:
                            clase, bits = 2, np.packbits(bloque).tobytes().hex()
                        w.writerow([y * 360 + x, clase, bits])
        grid = None


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("carpeta", type=Path)
    parser.add_argument("destino", type=Path)
    args = parser.parse_args()
    construir(args.carpeta, args.destino)

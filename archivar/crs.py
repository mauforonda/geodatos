"""Inferencia del CRS de capas cuyas coordenadas no corresponden al sistema que declaran.

Algunas fuentes declaran EPSG:4326 pero entregan metros. Para esos casos se prueban
unos pocos sistemas plausibles para Bolivia y se acepta uno solo si, con el, todas las
extensiones de las geometrias caen dentro del territorio nacional y ningun otro sistema
ubica dentro del pais al menos la mitad de ellas. Si hay varios, desempata la extension de
otras capas del mismo geoserver cuya extension es valida (`capas.csv`): gana el sistema
cuya extension coincide con la de alguna de ellas y con ninguna de las demas alternativas.
Si hay varios sin desempate o ninguno, no se adivina.

El limite nacional sale de la union de los departamentos de
https://archive.org/download/geodatosbolivia_umsa_iiag_limites_departamentos/dataset.geoparquet
(ver `construir_limite`) y se guarda simplificado en `limite_bolivia.geojson`.
"""

import csv
import json
from functools import lru_cache
from pathlib import Path
from typing import Optional

import numpy as np
import pyarrow.parquet as pq
import shapely
from pyproj import Transformer

LIMITE = Path(__file__).resolve().parent / "limite_bolivia.geojson"
CAPAS = Path(__file__).resolve().parent.parent / "descubrir" / "capas.csv"
# Los limites difieren entre fuentes y estan simplificados: se tolera un margen en grados (~11 km)
TOLERANCIA = 0.1
# Algunas capas correctas se salen del pais; un sistema es plausible si ubica dentro al menos esta fraccion
FRACCION_PLAUSIBLE = 0.5
# Dos extensiones coinciden si cada borde difiere menos que esto (grados, ~2 km)
TOLERANCIA_HERMANAS = 0.02
SIMPLIFICACION = 0.01
LOTE_GEOMETRIAS = 50_000
# Sistemas proyectados frecuentes en Bolivia: WGS 84 / UTM 19S, 20S y 21S, y Web Mercator
CANDIDATOS = ["EPSG:32719", "EPSG:32720", "EPSG:32721", "EPSG:3857"]


def construir_limite(departamentos_geoparquet: Path, destino: Path = LIMITE) -> None:
    import geopandas as gpd

    union = shapely.union_all(gpd.read_parquet(departamentos_geoparquet).to_crs(4326).geometry.values)
    simple = shapely.simplify(union, SIMPLIFICACION, preserve_topology=True)
    simple = shapely.set_precision(simple, 0.001)
    with open(destino, "w", encoding="utf-8") as f:
        json.dump(shapely.geometry.mapping(simple), f, separators=(",", ":"))
        f.write("\n")


@lru_cache(maxsize=1)
def limite_bolivia():
    with open(LIMITE, encoding="utf-8") as f:
        geom = shapely.geometry.shape(json.load(f)).buffer(TOLERANCIA)
    shapely.prepare(geom)
    return geom


def limites_de_geoparquet(path: Path, columna: str) -> np.ndarray:
    """Extension (minx, miny, maxx, maxy) de cada geometria."""
    partes = []
    for lote in pq.ParquetFile(path).iter_batches(columns=[columna], batch_size=LOTE_GEOMETRIAS):
        geoms = shapely.from_wkb(lote.column(0).to_numpy(zero_copy_only=False))
        partes.append(shapely.bounds(geoms))
    return np.vstack(partes) if partes else np.empty((0, 4))


@lru_cache(maxsize=1)
def extensiones_validas_por_geoserver(path: Path = CAPAS) -> dict[str, np.ndarray]:
    """Extensiones (minx, miny, maxx, maxy) de capas con coordenadas lon/lat dentro de Bolivia, por geoserver."""
    por_geoserver: dict[str, list[list[float]]] = {}
    with open(path, encoding="utf-8", newline="") as f:
        for fila in csv.DictReader(f):
            try:
                caja = [float(fila[k]) for k in ("min_x", "min_y", "max_x", "max_y")]
            except ValueError:
                continue
            centro = ((caja[0] + caja[2]) / 2, (caja[1] + caja[3]) / 2)
            if caja[0] <= caja[2] and caja[1] <= caja[3] and shapely.contains_xy(limite_bolivia(), *centro):
                por_geoserver.setdefault(fila["geoserver"], []).append(caja)
    return {geoserver: np.array(cajas) for geoserver, cajas in por_geoserver.items()}


def fraccion_dentro(crs: str, x: np.ndarray, y: np.ndarray) -> float:
    lon, lat = Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform(x, y)
    dentro = np.isfinite(lon) & np.isfinite(lat)
    dentro[dentro] = shapely.contains_xy(limite_bolivia(), lon[dentro], lat[dentro])
    return float(dentro.mean())


def coincide_con(crs: str, x: np.ndarray, y: np.ndarray, hermanas: np.ndarray) -> bool:
    lon, lat = Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform(x, y)
    if not (np.isfinite(lon).all() and np.isfinite(lat).all()):
        return False
    caja = np.array([lon.min(), lat.min(), lon.max(), lat.max()])
    return bool((np.abs(hermanas - caja).max(axis=1) <= TOLERANCIA_HERMANAS).any())


def inferir_crs(
    limites: np.ndarray,
    preferidos: Optional[list[str]] = None,
    hermanas: Optional[np.ndarray] = None,
) -> tuple[Optional[str], list[str]]:
    """Devuelve (crs, candidatos plausibles).

    Un CRS preferido (el que declara la fuente aparte de EPSG:4326) gana si ubica todo dentro de Bolivia. Si no,
    `crs` es el unico candidato plausible y debe ubicar todo dentro. Con varios plausibles, `hermanas` (extensiones
    validas de otras capas del mismo geoserver) desempata. Es None si no hay ninguno o no hay desempate.
    """
    limites = limites[~np.isnan(limites).any(axis=1)]
    if not len(limites):
        return None, []
    x = np.concatenate([limites[:, 0], limites[:, 0], limites[:, 2], limites[:, 2]])
    y = np.concatenate([limites[:, 1], limites[:, 3], limites[:, 1], limites[:, 3]])
    preferidos = preferidos or []
    fracciones = {crs: fraccion_dentro(crs, x, y) for crs in dict.fromkeys([*preferidos, *CANDIDATOS])}
    plausibles = [crs for crs, f in fracciones.items() if f >= FRACCION_PLAUSIBLE]
    elegido = next((crs for crs in preferidos if fracciones[crs] == 1.0), None)
    if elegido is None and len(plausibles) == 1 and fracciones[plausibles[0]] == 1.0:
        elegido = plausibles[0]
    if elegido is None and len(plausibles) > 1 and hermanas is not None and len(hermanas):
        respaldados = [crs for crs in plausibles if coincide_con(crs, x, y, hermanas)]
        if len(respaldados) == 1 and fracciones[respaldados[0]] == 1.0:
            elegido = respaldados[0]
    return elegido, plausibles

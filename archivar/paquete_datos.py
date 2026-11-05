"""Perfil determinista de un geoparquet archivado y su descriptor Data Package v2.

El descriptor usa solo propiedades del estandar y solo lo que sale de los datos
o de los metadatos de la fuente que ya guardamos. No se infiere significado:
tampoco licencias ni descripciones de columnas, que quedan fuera hasta que
exista una fuente real.
"""

import hashlib
import math
from datetime import date, datetime, time
from decimal import Decimal
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import duckdb
import pyarrow.parquet as pq
import shapely

from archivar.geoparquet import COLUMNA_BBOX, bbox_lonlat_valido, comillas, inspeccionar, suma_hashes

SCHEMA_URL = "https://datapackage.org/profiles/2.0/datapackage.json"
MAX_CATEGORIAS = 30
MAX_LARGO_TEXTO = 100
MIN_FILAS_RESTRICCIONES = 10
LOTE_GEOMETRIAS = 50_000
TIPOS_GEOMETRIA = {
    0: "Point",
    1: "LineString",
    2: "LinearRing",
    3: "Polygon",
    4: "MultiPoint",
    5: "MultiLineString",
    6: "MultiPolygon",
    7: "GeometryCollection",
}
DESCRIPCION_BBOX = (
    "Extensión de la geometría (xmin, ymin, xmax, ymax). Filtrar por esta columna permite "
    "leer solo los grupos de filas que intersectan el área de interés."
)


def tipo_datapackage(tipo: str) -> str:
    t = tipo.upper()
    if t.endswith("]") or t.startswith("LIST"):
        return "array"
    if t.startswith(("STRUCT", "MAP")):
        return "object"
    if t.startswith(("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "HUGEINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT", "UHUGEINT")):
        return "integer"
    if t.startswith(("FLOAT", "DOUBLE", "DECIMAL", "REAL")):
        return "number"
    if t == "BOOLEAN":
        return "boolean"
    if t == "DATE":
        return "date"
    if t.startswith("TIMESTAMP"):
        return "datetime"
    if t.startswith("TIME"):
        return "time"
    if t == "VARCHAR":
        return "string"
    return "any"


def valor_json(valor):
    if isinstance(valor, Decimal):
        valor = float(valor)
    if isinstance(valor, float):
        return valor if math.isfinite(valor) else None
    if isinstance(valor, (datetime, date, time)):
        return valor.isoformat()
    if valor is None or isinstance(valor, (bool, int, str)):
        return valor
    return str(valor)


def sha256_archivo(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloque)
    return h.hexdigest()


def perfilar_campo(con, path: Path, nombre: str, tipo: str, filas: int) -> dict:
    q = comillas(nombre)
    dp = tipo_datapackage(tipo)
    primitivo = dp in {"string", "integer", "number", "boolean", "date", "datetime", "time"}
    ordenable = dp in {"integer", "number", "date", "datetime", "time"}
    seleccion = [f"count(*) - count({q})"]
    if primitivo:
        seleccion.append(f"count(DISTINCT {q})")
    if ordenable:
        seleccion += [f"min({q})", f"max({q})"]
    if dp == "string":
        seleccion.append(f"max(length({q}))")
    fila = con.execute(f"SELECT {', '.join(seleccion)} FROM read_parquet(?)", [str(path)]).fetchone()
    nulos = fila[0]
    distintos = fila[1] if primitivo else None
    minimo, maximo = (valor_json(fila[2]), valor_json(fila[3])) if ordenable else (None, None)
    largo = fila[-1] if dp == "string" else None

    campo = {"name": nombre, "type": dp}
    restricciones = {}
    if filas >= MIN_FILAS_RESTRICCIONES:
        if nulos == 0:
            restricciones["required"] = True
        if distintos is not None and nulos == 0 and distintos == filas:
            restricciones["unique"] = True
    if minimo is not None and maximo is not None:
        restricciones["minimum"] = minimo
        restricciones["maximum"] = maximo
    if restricciones:
        campo["constraints"] = restricciones

    if dp == "string" and distintos and nulos < filas:
        ejemplo = con.execute(
            f"SELECT {q} FROM read_parquet(?) WHERE {q} IS NOT NULL LIMIT 1", [str(path)]
        ).fetchone()[0]
        campo["example"] = ejemplo[:MAX_LARGO_TEXTO]
        if distintos <= MAX_CATEGORIAS and filas >= 2 * distintos and (largo or 0) <= MAX_LARGO_TEXTO:
            # De la mas a la menos frecuente: la primera categoria es la mas comun
            campo["categories"] = [
                valor
                for valor, _ in con.execute(
                    f"SELECT {q}, count(*) FROM read_parquet(?) WHERE {q} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1",
                    [str(path)],
                ).fetchall()
            ]
    return campo


def perfilar_geometria(path: Path, columna: str) -> dict:
    tipos: dict[str, int] = {}
    nulas = invalidas = 0
    for lote in pq.ParquetFile(path).iter_batches(columns=[columna], batch_size=LOTE_GEOMETRIAS):
        geoms = shapely.from_wkb(lote.column(0).to_numpy(zero_copy_only=False))
        ausentes = shapely.is_missing(geoms)
        nulas += int(ausentes.sum())
        presentes = geoms[~ausentes]
        invalidas += int((~shapely.is_valid(presentes)).sum())
        for id_tipo in shapely.get_type_id(presentes):
            nombre = TIPOS_GEOMETRIA.get(int(id_tipo), str(id_tipo))
            tipos[nombre] = tipos.get(nombre, 0) + 1
    return {
        "tipos": sorted(tipos, key=lambda t: (-tipos[t], t)),
        "nulas": nulas,
        "invalidas": invalidas,
    }


def calcular_hash(path: Path, columnas: list[tuple[str, str]]) -> str:
    """Hash del contenido: mismo esquema y mismas filas (incluida la geometria), en cualquier orden."""
    utiles = [(c, t) for c, t in columnas if c != COLUMNA_BBOX]
    filas, suma = suma_hashes(path, [c for c, _ in utiles])
    esquema = "|".join(f"{c}:{t}" for c, t in sorted(utiles))
    return hashlib.sha256(f"{esquema}\n{filas}\n{suma}".encode("utf-8")).hexdigest()[:32]


def descripcion_geometria(geometria: dict, lonlat: bool, crs_inferido: Optional[str]) -> str:
    texto = "Geometría en WKB, EPSG:4326 (longitud, latitud)."
    if geometria["tipos"]:
        texto += f" Tipos: {', '.join(geometria['tipos'])}."
    if geometria["nulas"]:
        texto += f" Filas sin geometría: {geometria['nulas']}."
    if geometria["invalidas"]:
        texto += f" Geometrías inválidas, conservadas sin reparar: {geometria['invalidas']}."
    if crs_inferido:
        texto += (
            f" La fuente declaraba EPSG:4326 pero sus coordenadas estaban en {crs_inferido} "
            "(sistema inferido por la ubicación de las geometrías en Bolivia); se reproyectaron."
        )
    elif not lonlat:
        texto += (
            " Atención: la fuente declara EPSG:4326 pero las coordenadas no son longitud y latitud, "
            "y no conocemos el sistema real."
        )
    return texto


def perfilar(path: Path, crs_inferido: Optional[str] = None) -> dict:
    info = inspeccionar(path)
    geometria = perfilar_geometria(path, info["geometria"])
    lonlat = bbox_lonlat_valido(info["bbox_geo"] or [])
    con = duckdb.connect()
    try:
        columnas = con.execute(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet(?))", [str(path)]
        ).fetchall()
        campos = []
        for nombre, tipo in columnas:
            if nombre == info["geometria"]:
                campos.append(
                    {"name": nombre, "type": "any", "description": descripcion_geometria(geometria, lonlat, crs_inferido)}
                )
            elif nombre == COLUMNA_BBOX:
                campos.append({"name": nombre, "type": "object", "description": DESCRIPCION_BBOX})
            else:
                campos.append(perfilar_campo(con, path, nombre, tipo, info["filas"]))
    finally:
        con.close()
    esquema = pq.read_schema(path)
    return {
        "bytes": path.stat().st_size,
        "sha256": sha256_archivo(path),
        "hash": calcular_hash(path, columnas),
        "campos": campos,
        "coordenadas_lonlat": lonlat,
        "filas": info["filas"],
        "bbox": info["bbox_geo"],
        "geometria": info["geometria"],
        "tipos_geometria": geometria["tipos"],
        "tipos": {c: str(esquema.field(c).type) for c, _ in columnas if c not in {COLUMNA_BBOX}},
    }


def construir_datapackage(metadata: dict, perfil: dict) -> dict:
    item = metadata["archive_item"]
    titulo = (metadata.get("titulo") or metadata["nombre"]).strip()
    descripcion = (metadata.get("descripcion") or "").strip()
    ows = (metadata.get("ows") or "").strip()
    fuente = (metadata.get("fuente") or metadata.get("geoserver") or ows).strip()

    paquete = {"$schema": SCHEMA_URL, "name": item, "title": titulo}
    if descripcion:
        paquete["description"] = descripcion
    if ows:
        paquete["sources"] = [{"title": fuente, "path": ows}]
    paquete["resources"] = [
        {
            "name": "dataset",
            "type": "table",
            "path": f"https://archive.org/download/{quote(item)}/dataset.geoparquet",
            "format": "parquet",
            "mediatype": "application/vnd.apache.parquet",
            "bytes": perfil["bytes"],
            "hash": f"sha256:{perfil['sha256']}",
            "schema": {"fields": perfil["campos"]},
        }
    ]
    return paquete

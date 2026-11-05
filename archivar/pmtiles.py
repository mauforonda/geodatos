"""Teselas vectoriales (PMTiles) de un geoparquet normalizado.

El archivo se construye con tylertoo a partir del geoparquet en EPSG:4326. Los niveles de zoom salen de la
extension y de la cantidad de vertices de la capa:

- El primer nivel es el zoom en que la extension completa cabe en un mapa de 800 px.
- El ultimo nivel conserva la geometria completa. Se eligen niveles extra segun cuantos vertices caben en una
  tesela (`VERTICES_POR_TESELA`), hasta el zoom 14 del visor, y se ajusta segun el tamano real de las teselas.

Las teselas llevan solo propiedades utiles para colorear: numericas con al menos dos valores y textos con pocas
categorias. Se descartan identificadores y coordenadas, y los decimales se redondean a 3 cifras significativas.
"""

import hashlib
import json
import math
import re
import shutil
import tempfile
from pathlib import Path
from typing import Optional

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import shapely
import tylertoo

from archivar.geoparquet import COLUMNA_BBOX, bbox_lonlat_valido, comillas, inspeccionar

CAPA = "dataset"
ARCHIVO = "dataset.pmtiles"
ZOOM_MAXIMO_VISOR = 14
ANCHO_MAPA_PX = 800
TAMANO_TESELA_PX = 256
VERTICES_POR_TESELA = {"puntos": 20_000, "geometrias": 50_000}
MAX_BYTES_TESELA = 400_000
MAX_BYTES_ZOOM = 60_000_000
MAX_BYTES_PMTILES = 150_000_000
MAX_INTENTOS_ZOOM = 3
MAX_CATEGORIAS = 9
MAX_PROPIEDADES = 8
MAX_LARGO_CATEGORIA = 100
MAX_VALORES_NUMERO_BARATO = 256
FACTOR_PROPIEDADES = 2.0
MIN_BYTES_FACTOR = 1_000_000
CIFRAS_SIGNIFICATIVAS = 3
LOTE_GEOMETRIAS = 50_000
OPCIONES_OVERVIEW = {"polygon_thinning": 0.0, "collapse_square": True}
NOMBRE_COORDENADA = re.compile(
    r"^(x|y|z|lon|lng|long|lat|latitud|longitud|coor\w*|coord\w*|\w*_(x|y|lon|lat))$", re.IGNORECASE
)
NOMBRE_IDENTIFICADOR = re.compile(
    r"(^|_)(id|fid|gid|oid|objectid|ogc_fid|uuid|guid|cod|codigo|code|nro|num|numero)(_|$)", re.IGNORECASE
)
TIPOS_ENTEROS = ("TINYINT", "SMALLINT", "INTEGER", "BIGINT", "UTINYINT", "USMALLINT", "UINTEGER", "UBIGINT")
TIPOS_DECIMALES = ("FLOAT", "DOUBLE", "DECIMAL", "REAL")


class PmtilesOmitido(RuntimeError):
    """No se genera PMTiles para este dataset y reintentar no cambia el resultado."""

    tipo = "pmtiles"


def zoom_minimo(bbox: list[float]) -> int:
    """Zoom en que la extension completa cabe en un mapa de 800 px."""
    lado = max(bbox[2] - bbox[0], bbox[3] - bbox[1])
    if lado <= 0:
        return 12
    zoom = math.floor(math.log2(ANCHO_MAPA_PX * 360 / (TAMANO_TESELA_PX * lado)))
    return max(0, min(12, zoom))


def zoom_maximo(vertices: int, solo_puntos: bool, minimo: int) -> int:
    por_tesela = VERTICES_POR_TESELA["puntos" if solo_puntos else "geometrias"]
    extra = max(1, math.ceil(math.log(max(vertices, 1) / por_tesela, 4))) if vertices > por_tesela else 1
    return min(ZOOM_MAXIMO_VISOR, minimo + extra + 1)


def medir_geometrias(path: Path, columna: str) -> dict:
    vertices = no_vacias = 0
    solo_puntos = True
    for lote in pq.ParquetFile(path).iter_batches(columns=[columna], batch_size=LOTE_GEOMETRIAS):
        geoms = shapely.from_wkb(lote.column(0).to_numpy(zero_copy_only=False))
        presentes = geoms[~shapely.is_missing(geoms)]
        presentes = presentes[~shapely.is_empty(presentes)]
        no_vacias += len(presentes)
        vertices += int(shapely.get_num_coordinates(presentes).sum())
        solo_puntos = solo_puntos and bool(np.isin(shapely.get_type_id(presentes), [0, 4]).all())
    return {"vertices": vertices, "no_vacias": no_vacias, "solo_puntos": solo_puntos and no_vacias > 0}


def es_entero(tipo: str) -> bool:
    return tipo.upper().startswith(TIPOS_ENTEROS)


def es_decimal(tipo: str) -> bool:
    return tipo.upper().startswith(TIPOS_DECIMALES)


def elegir_propiedades(path: Path, columna_geometria: str) -> list[dict]:
    """Columnas para colorear: categoricas (2 a 9 valores) primero y luego numericas, hasta 8.

    Cada una lleva `nombre`, `tipo` ("string" o "number"), `distintos` y `categorias` o `minimo` y `maximo`.
    """
    con = duckdb.connect()
    try:
        columnas = con.execute(
            "SELECT column_name, column_type FROM (DESCRIBE SELECT * FROM read_parquet(?))", [str(path)]
        ).fetchall()
        candidatas = []
        for nombre, tipo in columnas:
            if nombre in (columna_geometria, COLUMNA_BBOX):
                continue
            if tipo.upper() == "VARCHAR":
                candidatas.append((nombre, tipo, "string"))
            elif es_entero(tipo) or es_decimal(tipo):
                if not NOMBRE_COORDENADA.match(nombre):
                    candidatas.append((nombre, tipo, "number"))
        if not candidatas:
            return []
        expresiones = ["count(*)"]
        for nombre, _, clase in candidatas:
            q = comillas(nombre)
            distintos = f"count(DISTINCT {q})" if clase == "string" else f"approx_count_distinct({q})"
            expresiones += [distintos, f"count({q})", f"min({q})", f"max({q})"]
        fila = con.execute(f"SELECT {', '.join(expresiones)} FROM read_parquet(?)", [str(path)]).fetchone()
        categoricas, numericas = [], []
        for i, (nombre, tipo, clase) in enumerate(candidatas):
            distintos, con_valor, minimo, maximo = fila[1 + 4 * i : 5 + 4 * i]
            if clase == "string":
                identificador = NOMBRE_IDENTIFICADOR.search(nombre) and distintos == con_valor
                if 2 <= distintos <= MAX_CATEGORIAS and not identificador:
                    categoricas.append({"nombre": nombre, "tipo": "string", "distintos": distintos})
                continue
            if distintos < 2 or NOMBRE_IDENTIFICADOR.search(nombre):
                continue
            # Una secuencia consecutiva de enteros sin repeticiones es un identificador
            if es_entero(tipo) and distintos >= 0.9 * con_valor and (maximo - minimo + 1) <= 1.1 * con_valor:
                continue
            numericas.append(
                {"nombre": nombre, "tipo": "number", "distintos": distintos, "minimo": float(minimo), "maximo": float(maximo)}
            )
        numericas.sort(key=lambda p: p["distintos"])
        elegidas = (categoricas + numericas)[:MAX_PROPIEDADES]
        for propiedad in elegidas:
            if propiedad["tipo"] == "string":
                q = comillas(propiedad["nombre"])
                valores = con.execute(
                    f"SELECT {q}, count(*) FROM read_parquet(?) WHERE {q} IS NOT NULL GROUP BY 1 ORDER BY 2 DESC, 1",
                    [str(path)],
                ).fetchall()
                propiedad["categorias"] = [v for v, _ in valores]
        # Textos largos inflan las teselas y no sirven de leyenda
        return [
            p
            for p in elegidas
            if p["tipo"] == "number" or all(len(c) <= MAX_LARGO_CATEGORIA for c in p["categorias"])
        ]
    finally:
        con.close()


def redondear_cifras(valores: pa.Array, cifras: int = CIFRAS_SIGNIFICATIVAS) -> pa.Array:
    x = valores.to_numpy(zero_copy_only=False).astype("float64")
    with np.errstate(all="ignore"):
        magnitud = np.floor(np.log10(np.abs(x)))
        magnitud[~np.isfinite(magnitud)] = 0
        escala = 10.0 ** (cifras - 1 - magnitud)
        redondeado = np.round(x * escala) / escala
    redondeado[~np.isfinite(x)] = x[~np.isfinite(x)]
    return pa.array(redondeado, mask=np.isnan(x))


def escribir_subconjunto(origen: Path, destino: Path, columna: str, propiedades: list[str]) -> None:
    """Geometria y propiedades elegidas, sin las demas columnas (tylertoo copia todas las que recibe)."""
    pf = pq.ParquetFile(origen)
    geo = json.loads(pf.metadata.metadata[b"geo"])
    tabla = pq.read_table(origen, columns=[columna, *propiedades])
    for i, campo in enumerate(tabla.schema):
        if pa.types.is_floating(campo.type):
            tabla = tabla.set_column(i, campo.name, redondear_cifras(tabla.column(i).combine_chunks()))
    geo["columns"][columna].pop("covering", None)
    meta = dict(tabla.schema.metadata or {})
    meta[b"geo"] = json.dumps(geo).encode()
    pq.write_table(tabla.replace_schema_metadata(meta), destino, compression="zstd")


def estadisticas_teselas(path: Path) -> dict:
    from pmtiles.reader import MmapSource, all_tiles

    maximo = n = 0
    with open(path, "rb") as f:
        for _, datos in all_tiles(MmapSource(f)):
            maximo = max(maximo, len(datos))
            n += 1
    return {"max_bytes_tesela": maximo, "teselas": n}


def leer_cabecera(path: Path) -> dict:
    from pmtiles.reader import MmapSource, Reader

    with open(path, "rb") as f:
        lector = Reader(MmapSource(f))
        cabecera = lector.header()
        metadata = lector.metadata()
    return {
        "min_zoom": cabecera["min_zoom"],
        "max_zoom": cabecera["max_zoom"],
        "centro": [cabecera["center_lon_e7"] / 1e7, cabecera["center_lat_e7"] / 1e7, cabecera["center_zoom"]],
        "capas": [capa["id"] for capa in metadata.get("vector_layers", [])],
    }


def construir_teselas(entrada: Path, destino: Path, minimo: int, maximo: int) -> None:
    resumen = destino.with_suffix(".overview.parquet")
    try:
        tylertoo.overview(str(entrada), str(resumen), min_zoom=minimo, max_zoom=maximo, **OPCIONES_OVERVIEW)
        tylertoo.export_pmtiles(str(resumen), str(destino), layer_name=CAPA)
    except RuntimeError as e:
        if "no output rows" in str(e):
            raise PmtilesOmitido("ninguna geometria sobrevive a la generalizacion") from e
        raise
    finally:
        resumen.unlink(missing_ok=True)


def sha256_archivo(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for bloque in iter(lambda: f.read(1024 * 1024), b""):
            h.update(bloque)
    return h.hexdigest()


def construir_pmtiles(geoparquet: Path, destino: Path, propiedades: Optional[list[dict]] = None) -> dict:
    """Escribe `destino` desde un geoparquet normalizado y devuelve sus metadatos.

    Lanza `PmtilesOmitido` si la capa no se puede teselar de forma util.
    """
    info = inspeccionar(geoparquet)
    if not bbox_lonlat_valido(info["bbox_geo"] or []):
        raise PmtilesOmitido(f"coordenadas fuera de lon/lat: {info['bbox_geo']}")
    medidas = medir_geometrias(geoparquet, info["geometria"])
    if not medidas["no_vacias"]:
        raise PmtilesOmitido("la capa no tiene geometrias")
    minimo = zoom_minimo(info["bbox_geo"])
    maximo = zoom_maximo(medidas["vertices"], medidas["solo_puntos"], minimo)
    if propiedades is None:
        propiedades = elegir_propiedades(geoparquet, info["geometria"])

    with tempfile.TemporaryDirectory(prefix="geodatos-pmtiles-") as tmpdir:
        tmp = Path(tmpdir)

        def construir(zoom_max: int, props: list[dict], nombre: str) -> dict:
            entrada, salida = tmp / f"{nombre}.parquet", tmp / f"{nombre}.pmtiles"
            escribir_subconjunto(geoparquet, entrada, info["geometria"], [p["nombre"] for p in props])
            construir_teselas(entrada, salida, minimo, zoom_max)
            entrada.unlink()
            return {"zoom_max": zoom_max, "path": salida, "bytes": salida.stat().st_size, **estadisticas_teselas(salida)}

        def aceptable(r: dict) -> bool:
            return r["bytes"] <= MAX_BYTES_ZOOM and r["max_bytes_tesela"] <= MAX_BYTES_TESELA

        # Se ajusta el zoom final al tamano real de las teselas
        intentos = []
        for n in range(MAX_INTENTOS_ZOOM):
            r = construir(maximo, propiedades, f"zoom{n}")
            intentos.append(r)
            if aceptable(r):
                break
            if r["max_bytes_tesela"] > MAX_BYTES_TESELA and maximo < ZOOM_MAXIMO_VISOR and r["bytes"] <= MAX_BYTES_ZOOM:
                maximo += 1
            elif r["bytes"] > MAX_BYTES_ZOOM and maximo > minimo + 1:
                maximo -= 1
            else:
                break
            if any(i["zoom_max"] == maximo for i in intentos):
                break
        elegido = next((r for r in intentos if aceptable(r)), min(intentos, key=lambda r: r["bytes"]))

        # Las propiedades no deben pesar mas que la geometria misma
        if propiedades and elegido["bytes"] > MIN_BYTES_FACTOR:
            base = construir(elegido["zoom_max"], [], "geometria")
            baratas = [p for p in propiedades if p["tipo"] == "string" or p["distintos"] <= MAX_VALORES_NUMERO_BARATO]
            categorias = [p for p in propiedades if p["tipo"] == "string"]
            for reducidas in (baratas, categorias):
                if elegido["bytes"] <= FACTOR_PROPIEDADES * base["bytes"]:
                    break
                if len(reducidas) == len(propiedades):
                    continue
                propiedades = reducidas
                elegido = construir(elegido["zoom_max"], propiedades, "reducida") if propiedades else base
        if elegido["bytes"] > MAX_BYTES_PMTILES:
            raise PmtilesOmitido(f"el archivo pesa {elegido['bytes']} bytes, mas que el maximo {MAX_BYTES_PMTILES}")
        shutil.copyfile(elegido["path"], destino)

    cabecera = leer_cabecera(destino)
    return {
        "bytes": destino.stat().st_size,
        "sha256": sha256_archivo(destino),
        "min_zoom": cabecera["min_zoom"],
        "max_zoom": cabecera["max_zoom"],
        "centro": cabecera["centro"],
        "capa": CAPA,
        "max_bytes_tesela": elegido["max_bytes_tesela"],
        "propiedades": [
            {k: p[k] for k in ("nombre", "tipo", "categorias", "minimo", "maximo") if k in p} for p in propiedades
        ],
    }

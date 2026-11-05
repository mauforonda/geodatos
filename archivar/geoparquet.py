"""Lectura de metadatos de geoparquets y verificacion del formato normalizado."""

import json
from pathlib import Path
from typing import Optional

import duckdb
import pyarrow.parquet as pq

MAX_FILAS_GRUPO = 49_152
COLUMNA_BBOX = "bbox"


def crs_es_4326(crs: Optional[dict]) -> bool:
    if not crs:
        return False
    ident = crs.get("id") or {}
    if str(ident.get("authority", "")).upper() in {"EPSG", "OGC"}:
        return str(ident.get("code")) in {"4326", "CRS84"}
    return False


def inspeccionar(path: Path) -> dict:
    """Lee el footer: no carga datos."""
    pf = pq.ParquetFile(path)
    meta = pf.metadata
    geo = json.loads((meta.metadata or {}).get(b"geo", b"{}"))
    columna = geo.get("primary_column", "geometry")
    info_columna = (geo.get("columns") or {}).get(columna, {})
    crs_declarado = "crs" in info_columna
    grupos = [meta.row_group(i) for i in range(meta.num_row_groups)]
    indice = pf.schema_arrow.get_field_index(columna)
    compresion = {g.column(indice).compression for g in grupos} if indice >= 0 and grupos else set()
    return {
        "filas": meta.num_rows,
        "version": geo.get("version", ""),
        "geometria": columna,
        "crs": info_columna.get("crs"),
        "crs_declarado": crs_declarado,
        "crs_4326": (not crs_declarado) or crs_es_4326(info_columna.get("crs")),
        "bbox_geo": info_columna.get("bbox"),
        "covering": COLUMNA_BBOX in (info_columna.get("covering") or {}),
        "compresion": compresion,
        "grupos": len(grupos),
        "max_filas_grupo": max((g.num_rows for g in grupos), default=0),
        "columnas": list(pf.schema_arrow.names),
    }


def esta_normalizado(info: dict) -> bool:
    return (
        info["version"].startswith("1.1")
        and info["crs_4326"]
        and info["covering"]
        and info["compresion"] == {"ZSTD"}
        and info["max_filas_grupo"] <= MAX_FILAS_GRUPO
    )


def bbox_lonlat_valido(bbox) -> bool:
    return (
        bool(bbox)
        and len(bbox) >= 4
        and -180 <= bbox[0] <= bbox[2] <= 180
        and -90 <= bbox[1] <= bbox[3] <= 90
    )


def comillas(nombre: str) -> str:
    return '"' + nombre.replace('"', '""') + '"'


def suma_hashes(path: Path, columnas: list[str]) -> tuple[int, str]:
    """Numero de filas y suma de hashes de las columnas dadas (independiente del orden de filas)."""
    con = duckdb.connect()
    try:
        if columnas:
            lista = ", ".join(comillas(c) for c in sorted(columnas))
            expresion = f"coalesce(sum(hash({lista})::HUGEINT), 0)"
        else:
            expresion = "0"
        filas, suma = con.execute(
            f"SELECT count(*), {expresion} FROM read_parquet(?)", [str(path)]
        ).fetchone()
    finally:
        con.close()
    return filas, str(suma)

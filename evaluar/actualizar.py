#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import math
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import pytz
import urllib3
import xmltodict


BASE_DIR = Path(__file__).resolve().parent
DESCUBRIR_DIR = BASE_DIR.parent / "descubrir"

CAPAS = DESCUBRIR_DIR / "capas.csv"
LOG = DESCUBRIR_DIR / "log.csv"
DIRECTORIO = DESCUBRIR_DIR / "directorio.json"
DATASETS = BASE_DIR / "datasets.csv"

COLUMNAS = [
    "geoserver",
    "nombre",
    "descripcion",
    "wfs_activo",
    "fallas_wfs_90d",
    "bbox_area",
    "n_features",
    "bytes_estimados",
    "geometria_valida",
    "errores_evaluar",
    "errores_archivar",
    "archivado",
    "fecha_ultima_evaluacion",
    "fecha_ultimo_archivo_intento",
    "fecha_archivado",
]

TIMEZONE = pytz.timezone("America/La_Paz")
thread_local = threading.local()


def iniciar_sesion() -> urllib3.PoolManager:
    sesion = getattr(thread_local, "sesion", None)
    if sesion is None:
        urllib3.disable_warnings()
        sesion = urllib3.PoolManager(
            timeout=urllib3.util.Timeout(connect=8.0, read=20.0),
            retries=urllib3.Retry(total=2, redirect=False),
            cert_reqs="CERT_NONE",
            num_pools=32,
            maxsize=32,
            headers={"User-Agent": "geodatos-evaluar/1.0"},
        )
        thread_local.sesion = sesion
    return sesion


def hoy() -> str:
    return dt.datetime.now(TIMEZONE).date().isoformat()


def leer_capas_actuales() -> pd.DataFrame:
    capas = pd.read_csv(CAPAS, keep_default_na=False)
    capas["wfs"] = capas["wfs"].astype(str).eq("True")
    capas["fecha_removido"] = capas["fecha_removido"].replace("", pd.NA)

    activas = capas[(capas["wfs"]) & (capas["fecha_removido"].isna())].copy()
    activas["descripcion"] = activas["descripcion"].fillna("").astype(str)
    activas["bbox_area"] = (
        pd.to_numeric(activas["max_x"], errors="coerce")
        - pd.to_numeric(activas["min_x"], errors="coerce")
    ) * (
        pd.to_numeric(activas["max_y"], errors="coerce")
        - pd.to_numeric(activas["min_y"], errors="coerce")
    )
    activas["bbox_area"] = activas["bbox_area"].abs()
    activas["bbox_area"] = activas["bbox_area"].replace([float("inf"), float("-inf")], pd.NA)
    return activas[["geoserver", "nombre", "descripcion", "bbox_area"]].copy()


def leer_fallas_wfs() -> pd.DataFrame:
    log = pd.read_csv(LOG, parse_dates=["tiempo"])
    inicio = pd.Timestamp(dt.datetime.now(TIMEZONE).date() - dt.timedelta(days=90))

    fallas = log[
        (log["servicio"] == "wfs")
        & (log["evento"] == "error")
        & (log["tiempo"].dt.tz_localize(None) >= inicio)
    ].copy()

    if fallas.empty:
        return pd.DataFrame({"geoserver": [], "fallas_wfs_90d": []})

    fallas["fecha"] = fallas["tiempo"].dt.date
    return (
        fallas.groupby("geoserver")["fecha"]
        .nunique()
        .rename("fallas_wfs_90d")
        .reset_index()
    )


def leer_directorio() -> Dict[str, dict]:
    with open(DIRECTORIO, "r") as f:
        directorio = json.load(f)
    return {entrada["nombre"]: entrada for entrada in directorio}


def leer_existentes() -> pd.DataFrame:
    if not DATASETS.exists():
        return pd.DataFrame(columns=COLUMNAS)

    existentes = pd.read_csv(DATASETS, keep_default_na=False)
    for columna in COLUMNAS:
        if columna not in existentes.columns:
            existentes[columna] = ""

    for columna in ["wfs_activo", "archivado"]:
        existentes[columna] = existentes[columna].astype(str).eq("True")
    existentes["geometria_valida"] = (
        existentes["geometria_valida"]
        .astype(str)
        .str.strip()
        .map({"True": True, "False": False})
        .astype("boolean")
    )

    for columna in ["errores_evaluar", "errores_archivar", "fallas_wfs_90d"]:
        existentes[columna] = pd.to_numeric(existentes[columna], errors="coerce").fillna(0)

    for columna in ["bbox_area", "n_features", "bytes_estimados"]:
        existentes[columna] = pd.to_numeric(existentes[columna], errors="coerce")

    return existentes[COLUMNAS].copy()


def construir_base(
    existentes: pd.DataFrame,
    activas: pd.DataFrame,
    fallas: pd.DataFrame,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    activas = activas.copy()
    activas["wfs_activo"] = True

    if existentes.empty:
        base = activas.copy()
        base["fallas_wfs_90d"] = 0
        base["n_features"] = pd.NA
        base["bytes_estimados"] = pd.NA
        base["geometria_valida"] = pd.Series(pd.NA, index=base.index, dtype="boolean")
        base["errores_evaluar"] = 0
        base["errores_archivar"] = 0
        base["archivado"] = False
        base["fecha_ultima_evaluacion"] = ""
        base["fecha_ultimo_archivo_intento"] = ""
        base["fecha_archivado"] = ""
    else:
        base = existentes.merge(
            activas,
            on=["geoserver", "nombre"],
            how="outer",
            suffixes=("_old", ""),
            indicator=True,
        )

        base["descripcion"] = base["descripcion"].where(
            base["descripcion"].notna() & base["descripcion"].astype(str).ne(""),
            base["descripcion_old"],
        )
        base["bbox_area"] = base["bbox_area"].where(
            base["bbox_area"].notna(),
            base["bbox_area_old"],
        )
        base["wfs_activo"] = base["wfs_activo"].astype("boolean").fillna(False).astype(bool)
        base["fallas_wfs_90d"] = base["fallas_wfs_90d"].fillna(0)
        base["errores_evaluar"] = base["errores_evaluar"].fillna(0)
        base["errores_archivar"] = base["errores_archivar"].fillna(0)
        base["archivado"] = base["archivado"].astype("boolean").fillna(False).astype(bool)
        base["geometria_valida"] = base["geometria_valida"].astype("boolean")
        for columna in [
            "fecha_ultima_evaluacion",
            "fecha_ultimo_archivo_intento",
            "fecha_archivado",
        ]:
            base[columna] = base[columna].fillna("")

        base = base[COLUMNAS].copy()

    base = base.merge(fallas, on="geoserver", how="left", suffixes=("", "_nuevo"))
    base["fallas_wfs_90d"] = base["fallas_wfs_90d_nuevo"].fillna(base["fallas_wfs_90d"]).fillna(0)
    base = base.drop(columns=["fallas_wfs_90d_nuevo"])

    base["descripcion"] = base["descripcion"].fillna("").astype(str)
    pendientes = base.loc[
        base["wfs_activo"].fillna(False)
        & ~base["archivado"].fillna(False)
        & base["geometria_valida"].isna(),
        ["geoserver", "nombre"],
    ].copy()
    return base, pendientes


def consultar_hits(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
) -> Optional[int]:
    respuesta = sesion.request(
        "GET",
        ows,
        fields={
            "service": "wfs",
            "version": "1.1.0",
            "request": "GetFeature",
            "typeName": nombre,
            "resultType": "hits",
        },
    )
    if respuesta.status != 200:
        return None

    data = xmltodict.parse(respuesta.data)
    posibles = [
        data.get("wfs:FeatureCollection", {}),
        data.get("FeatureCollection", {}),
    ]
    for candidato in posibles:
        for atributo in ["@numberOfFeatures", "@numberMatched"]:
            valor = candidato.get(atributo)
            if valor not in [None, "unknown"]:
                try:
                    return int(valor)
                except ValueError:
                    return None
    return None


def consultar_muestra_geojson(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
) -> Optional[Tuple[int, int, bool]]:
    respuesta = sesion.request(
        "GET",
        ows,
        fields={
            "service": "wfs",
            "version": "1.0.0",
            "request": "GetFeature",
            "typeName": nombre,
            "outputFormat": "application/json",
            "srsName": "EPSG:4326",
            "maxFeatures": 5,
        },
    )
    if respuesta.status != 200:
        return None

    try:
        data = json.loads(respuesta.data)
    except json.JSONDecodeError:
        return None

    if data.get("type") != "FeatureCollection" or not isinstance(data.get("features"), list):
        return None

    features = data["features"]
    n_features = len(features)
    if n_features == 0:
        return None
    return len(respuesta.data), n_features, muestra_tiene_geometrias_validas(features)


def iterar_posiciones(coordenadas):
    if not isinstance(coordenadas, list):
        raise ValueError("Las coordenadas GeoJSON deben ser listas")
    if coordenadas and all(isinstance(valor, (int, float)) and not isinstance(valor, bool) for valor in coordenadas):
        if len(coordenadas) < 2:
            raise ValueError("Una posicion GeoJSON debe tener al menos dos coordenadas")
        yield coordenadas
        return
    for elemento in coordenadas:
        yield from iterar_posiciones(elemento)


def iterar_geometrias(geometria):
    if geometria is None:
        return
    if not isinstance(geometria, dict):
        raise ValueError("La geometria GeoJSON debe ser un objeto")
    if geometria.get("type") == "GeometryCollection":
        geometrias = geometria.get("geometries")
        if not isinstance(geometrias, list):
            raise ValueError("GeometryCollection debe contener una lista de geometrias")
        for elemento in geometrias:
            yield from iterar_geometrias(elemento)
        return
    if geometria.get("type") not in {
        "Point",
        "MultiPoint",
        "LineString",
        "MultiLineString",
        "Polygon",
        "MultiPolygon",
    }:
        raise ValueError("Tipo de geometria GeoJSON desconocido")
    yield geometria


def muestra_tiene_geometrias_validas(features: list[dict]) -> bool:
    tiene_posiciones = False
    try:
        for feature in features:
            if not isinstance(feature, dict) or feature.get("type") != "Feature":
                return False
            for geometria in iterar_geometrias(feature.get("geometry")):
                for posicion in iterar_posiciones(geometria.get("coordinates")):
                    tiene_posiciones = True
                    if not all(math.isfinite(float(valor)) for valor in posicion):
                        return False
                    longitud, latitud = posicion[:2]
                    if not (-180 <= longitud <= 180 and -90 <= latitud <= 90):
                        return False
    except (TypeError, ValueError, OverflowError):
        return False
    return tiene_posiciones


def evaluar_dataset(
    directorio: Dict[str, dict],
    geoserver: str,
    nombre: str,
) -> Tuple[Optional[int], Optional[int], Optional[bool], bool]:
    entrada = directorio.get(geoserver)
    if not entrada:
        return None, None, None, False

    sesion = iniciar_sesion()
    ows = entrada["ows"]
    n_features = consultar_hits(sesion, ows, nombre)
    muestra = consultar_muestra_geojson(sesion, ows, nombre)

    if muestra is None:
        return n_features, None, None, False

    muestra_bytes, muestra_features, geometria_valida = muestra
    if n_features is None:
        return None, None, geometria_valida, True

    bytes_estimados = int(round((muestra_bytes / muestra_features) * n_features))
    return n_features, bytes_estimados, geometria_valida, True


def evaluar_fila(
    directorio: Dict[str, dict],
    geoserver: str,
    nombre: str,
) -> dict:
    try:
        n_features, bytes_estimados, geometria_valida, exito = evaluar_dataset(
            directorio,
            geoserver,
            nombre,
        )
    except Exception:
        n_features, bytes_estimados, geometria_valida, exito = None, None, None, False
    return {
        "geoserver": geoserver,
        "nombre": nombre,
        "n_features": n_features,
        "bytes_estimados": bytes_estimados,
        "geometria_valida": geometria_valida,
        "exito": exito,
    }


def aplicar_resultados(base: pd.DataFrame, resultados: list[dict], fecha: str) -> pd.DataFrame:
    if not resultados:
        return base

    resultados_df = pd.DataFrame(resultados)
    if resultados_df.empty:
        return base

    resultados_df["fecha_ultima_evaluacion"] = fecha
    base = base.merge(
        resultados_df,
        on=["geoserver", "nombre"],
        how="left",
        suffixes=("", "_nuevo"),
    )

    for columna in ["n_features", "bytes_estimados", "geometria_valida", "fecha_ultima_evaluacion"]:
        base[columna] = base[f"{columna}_nuevo"].where(
            base[f"{columna}_nuevo"].notna(),
            base[columna],
        )

    exito = base["exito"].astype("boolean").fillna(True)
    incremento_error = (~exito).astype(int)
    base["errores_evaluar"] = (
        pd.to_numeric(base["errores_evaluar"], errors="coerce").fillna(0) + incremento_error
    )

    columnas_aux = [
        "n_features_nuevo",
        "bytes_estimados_nuevo",
        "geometria_valida_nuevo",
        "fecha_ultima_evaluacion_nuevo",
        "exito",
    ]
    return base.drop(columns=[c for c in columnas_aux if c in base.columns])


def actualizar_pendientes(
    base: pd.DataFrame,
    pendientes: pd.DataFrame,
    directorio: Dict[str, dict],
    limite: Optional[int] = None,
    workers: int = 8,
    batch_size: int = 100,
) -> pd.DataFrame:
    if pendientes.empty:
        return base

    pendientes = pendientes.sort_values(["geoserver", "nombre"], kind="mergesort").reset_index(drop=True)
    if limite is not None:
        pendientes = pendientes.head(limite).copy()

    fecha = hoy()
    total = len(pendientes)
    resultados_lote: list[dict] = []

    with ThreadPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(evaluar_fila, directorio, fila.geoserver, fila.nombre)
            for fila in pendientes.itertuples(index=False)
        ]

        for i, future in enumerate(as_completed(futures), start=1):
            resultados_lote.append(future.result())

            if len(resultados_lote) >= batch_size or i == total:
                base = aplicar_resultados(base, resultados_lote, fecha)
                base = ordenar(base)
                guardar(base)
                resultados_lote = []

            if i % 100 == 0 or i == total:
                print(f"evaluados {i} datasets pendientes")

    return base


def ordenar(base: pd.DataFrame) -> pd.DataFrame:
    base = base.copy()
    base["tiene_descripcion"] = base["descripcion"].astype(str).str.strip().ne("")
    base["errores_totales"] = (
        pd.to_numeric(base["errores_evaluar"], errors="coerce").fillna(0)
        + pd.to_numeric(base["errores_archivar"], errors="coerce").fillna(0)
    )
    base["bytes_estimados_orden"] = pd.to_numeric(base["bytes_estimados"], errors="coerce").fillna(float("inf"))
    base["bbox_area_orden"] = pd.to_numeric(base["bbox_area"], errors="coerce").fillna(-1.0)
    base["n_features_orden"] = pd.to_numeric(base["n_features"], errors="coerce").fillna(-1.0)

    base = base.sort_values(
        by=[
            "wfs_activo",
            "archivado",
            "fallas_wfs_90d",
            "tiene_descripcion",
            "bytes_estimados_orden",
            "bbox_area_orden",
            "n_features_orden",
            "errores_totales",
            "geoserver",
            "nombre",
        ],
        ascending=[False, True, False, False, True, False, False, True, True, True],
        kind="mergesort",
    )

    return base.drop(
        columns=[
            "tiene_descripcion",
            "errores_totales",
            "bytes_estimados_orden",
            "bbox_area_orden",
            "n_features_orden",
        ]
    )


def guardar(base: pd.DataFrame) -> None:
    salida = base.copy()
    salida["fallas_wfs_90d"] = pd.to_numeric(salida["fallas_wfs_90d"], errors="coerce").fillna(0).astype(int)
    salida["errores_evaluar"] = pd.to_numeric(salida["errores_evaluar"], errors="coerce").fillna(0).astype(int)
    salida["errores_archivar"] = pd.to_numeric(salida["errores_archivar"], errors="coerce").fillna(0).astype(int)
    salida["wfs_activo"] = salida["wfs_activo"].astype(bool)
    salida["archivado"] = salida["archivado"].astype(bool)
    salida["geometria_valida"] = salida["geometria_valida"].astype("boolean")

    for columna in ["bbox_area", "n_features", "bytes_estimados"]:
        salida[columna] = pd.to_numeric(salida[columna], errors="coerce")
        salida[columna] = salida[columna].replace([float("inf"), float("-inf")], pd.NA)

    for columna in ["descripcion", "fecha_ultima_evaluacion", "fecha_ultimo_archivo_intento", "fecha_archivado"]:
        salida[columna] = salida[columna].fillna("")

    salida = salida[COLUMNAS]
    salida.to_csv(
        DATASETS,
        index=False,
        float_format="%.6f",
        date_format="%Y-%m-%d",
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--limite-pendientes",
        "--limite-nuevas",
        dest="limite_pendientes",
        type=int,
        default=None,
        help="Evalua solo esta cantidad de datasets pendientes. Util para pruebas manuales.",
    )
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--batch-size", type=int, default=100)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    activas = leer_capas_actuales()
    fallas = leer_fallas_wfs()
    existentes = leer_existentes()
    directorio = leer_directorio()

    base, pendientes = construir_base(existentes, activas, fallas)
    base = ordenar(base)
    guardar(base)
    print(f"capas activas WFS: {len(activas)}")
    print(f"datasets pendientes por evaluar: {len(pendientes)}")
    base = actualizar_pendientes(
        base,
        pendientes,
        directorio,
        limite=args.limite_pendientes,
        workers=args.workers,
        batch_size=args.batch_size,
    )
    base = ordenar(base)
    guardar(base)


if __name__ == "__main__":
    main()

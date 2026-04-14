#!/usr/bin/env python3

import argparse
import hashlib
import json
import math
import os
import re
import shutil
import signal
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import quote

import geopandas as gpd
import pandas as pd
import pytz
import urllib3
import xmltodict

BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
DESCUBRIR_DIR = ROOT_DIR / "descubrir"
EVALUAR_DIR = ROOT_DIR / "evaluar"

if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from archivar.componer_mapa import componer_mapa

CAPAS = DESCUBRIR_DIR / "capas.csv"
DIRECTORIO = DESCUBRIR_DIR / "directorio.json"
DATASETS = EVALUAR_DIR / "datasets.csv"
PAQUETES = BASE_DIR / "paquetes.csv"
RUNS = BASE_DIR / "runs.csv"
PUBLICADOS = BASE_DIR / "publicados"
TMP_STAGE = BASE_DIR / ".tmp_stage"
BLACKLIST = BASE_DIR / "blacklist.json"

TIMEZONE = pytz.timezone("America/La_Paz")
ITEM_PREFIX = "geodatosbolivia"
MAX_ERRORES_ARCHIVAR = 3
WFS_PAGE_SIZE = 5000
STREAM_CHUNK_SIZE = 1024 * 1024
DOWNLOAD_HEARTBEAT_SECONDS = 15


class DatasetTimeoutError(TimeoutError):
    pass

PAQUETES_COLUMNAS = [
    "geoserver",
    "nombre",
    "titulo",
    "descripcion",
    "fecha_archivado",
    "archive_item",
    "epsg",
    "bbox_area",
    "n_features",
    "bytes_geojson",
    "bytes_geoparquet",
    "tiene_legend_png",
    "tiene_map_png",
    "tiene_sample_json",
]

RUNS_COLUMNAS = [
    "geoserver",
    "nombre",
    "fecha_inicio",
    "fecha_fin",
    "segundos",
    "estado",
    "detalle",
]


def ahora() -> pd.Timestamp:
    return pd.Timestamp.now(tz=TIMEZONE)


def hoy() -> str:
    return ahora().date().isoformat()


def iniciar_sesion() -> urllib3.PoolManager:
    urllib3.disable_warnings()
    return urllib3.PoolManager(
        timeout=60,
        retries=2,
        cert_reqs="CERT_NONE",
    )


def slug(texto: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", texto.strip())).strip("_").lower()


def item_name(geoserver: str, nombre: str) -> str:
    return f"{ITEM_PREFIX}_{slug(geoserver)}_{slug(nombre)}"


def archive_metadata_url(item: str) -> str:
    return f"https://archive.org/metadata/{quote(item)}"


def archive_download_url(item: str, filename: str) -> str:
    return f"https://archive.org/download/{quote(item)}/{quote(filename)}"


def rutas_publicacion(geoserver: str, nombre: str) -> Dict[str, Path]:
    base = PUBLICADOS / slug(geoserver) / slug(nombre)
    return {
        "base": base,
        "metadata": base / "metadata.json",
        "legend": base / "legend.png",
        "map": base / "map.png",
        "sample": base / "sample.json",
    }


def parse_bool(serie: pd.Series) -> pd.Series:
    return serie.astype(str).eq("True")


def texto_o_vacio(valor) -> str:
    if valor is None or pd.isna(valor):
        return ""
    return str(valor).strip()


def titulo_legible(capa: pd.Series) -> str:
    titulo = texto_o_vacio(capa.titulo)
    if titulo:
        return titulo
    return texto_o_vacio(capa.nombre)


def descripcion_legible(capa: pd.Series) -> str:
    descripcion = texto_o_vacio(capa.descripcion)
    if descripcion:
        return descripcion
    return "No disponible."


def leer_datasets() -> pd.DataFrame:
    if not DATASETS.exists():
        raise FileNotFoundError(f"No existe {DATASETS}")

    datasets = pd.read_csv(DATASETS, keep_default_na=False)
    for columna in ["wfs_activo", "archivado"]:
        if columna in datasets.columns:
            datasets[columna] = parse_bool(datasets[columna])
        else:
            datasets[columna] = False

    for columna in ["errores_archivar", "errores_evaluar"]:
        if columna in datasets.columns:
            datasets[columna] = pd.to_numeric(datasets[columna], errors="coerce").fillna(0).astype(int)

    if "bytes_estimados" in datasets.columns:
        datasets["bytes_estimados"] = pd.to_numeric(datasets["bytes_estimados"], errors="coerce")
    else:
        datasets["bytes_estimados"] = pd.NA

    if "fecha_ultima_evaluacion" not in datasets.columns:
        datasets["fecha_ultima_evaluacion"] = ""
    if "fecha_ultimo_archivo_intento" not in datasets.columns:
        datasets["fecha_ultimo_archivo_intento"] = ""

    return datasets


def leer_capas() -> pd.DataFrame:
    capas = pd.read_csv(CAPAS, keep_default_na=False)
    capas["wfs"] = parse_bool(capas["wfs"])
    capas["fecha_removido"] = capas["fecha_removido"].replace("", pd.NA)
    return capas[(capas["wfs"]) & (capas["fecha_removido"].isna())].copy()


def leer_directorio() -> Dict[str, dict]:
    with open(DIRECTORIO, "r") as f:
        directorio = json.load(f)
    return {entrada["nombre"]: entrada for entrada in directorio}


def leer_paquetes() -> pd.DataFrame:
    if not PAQUETES.exists():
        return pd.DataFrame(columns=PAQUETES_COLUMNAS)
    paquetes = pd.read_csv(PAQUETES, keep_default_na=False)
    for columna in PAQUETES_COLUMNAS:
        if columna not in paquetes.columns:
            paquetes[columna] = ""
    for columna in ["tiene_legend_png", "tiene_map_png", "tiene_sample_json"]:
        paquetes[columna] = parse_bool(paquetes[columna])
    return paquetes[PAQUETES_COLUMNAS].copy()


def leer_blacklist() -> set[str]:
    if not BLACKLIST.exists():
        return set()

    with open(BLACKLIST, "r") as f:
        data = json.load(f)

    if isinstance(data, dict):
        geoservers = data.get("geoservers", [])
    elif isinstance(data, list):
        geoservers = data
    else:
        geoservers = []

    return {str(valor).strip() for valor in geoservers if str(valor).strip()}


def leer_runs() -> pd.DataFrame:
    if not RUNS.exists():
        return pd.DataFrame(columns=RUNS_COLUMNAS)
    runs = pd.read_csv(RUNS, keep_default_na=False)
    for columna in RUNS_COLUMNAS:
        if columna not in runs.columns:
            runs[columna] = ""
    return runs[RUNS_COLUMNAS].copy()


def limpiar_stage_huerfano(max_age_hours: int = 24) -> None:
    if not TMP_STAGE.exists():
        return

    ahora_ts = time.time()
    for child in TMP_STAGE.iterdir():
        try:
            age_hours = (ahora_ts - child.stat().st_mtime) / 3600
        except FileNotFoundError:
            continue
        if age_hours >= max_age_hours:
            if child.is_dir():
                shutil.rmtree(child, ignore_errors=True)
            else:
                child.unlink(missing_ok=True)


def seleccionar_candidatos(
    datasets: pd.DataFrame,
    geoservers_excluidos: set[str],
    max_errores_archivar: int = MAX_ERRORES_ARCHIVAR,
) -> pd.DataFrame:
    candidatos = datasets[
        (datasets["wfs_activo"])
        & (~datasets["archivado"])
        & (datasets["fecha_ultima_evaluacion"].fillna("").astype(str).ne(""))
        & (pd.to_numeric(datasets["bytes_estimados"], errors="coerce").notna())
        & (pd.to_numeric(datasets["errores_archivar"], errors="coerce").fillna(0) < max_errores_archivar)
    ].copy()
    if geoservers_excluidos:
        candidatos = candidatos[~candidatos["geoserver"].isin(sorted(geoservers_excluidos))].copy()
    return candidatos


def seleccionar_capa(capas: pd.DataFrame, geoserver: str, nombre: str) -> pd.Series:
    fila = capas[(capas["geoserver"] == geoserver) & (capas["nombre"] == nombre)]
    if fila.empty:
        raise KeyError(f"No se encontro metadata para {geoserver} / {nombre}")
    return fila.iloc[0]


def bbox_area(capa: pd.Series) -> Optional[float]:
    try:
        area = abs((float(capa.max_x) - float(capa.min_x)) * (float(capa.max_y) - float(capa.min_y)))
    except Exception:
        return None
    if math.isinf(area) or math.isnan(area):
        return None
    return area


def bbox_expandido(capa: pd.Series, padding_ratio: float = 0.15) -> Optional[str]:
    try:
        min_x = float(capa.min_x)
        max_x = float(capa.max_x)
        min_y = float(capa.min_y)
        max_y = float(capa.max_y)
    except Exception:
        return None

    dx = max(max_x - min_x, 0.001)
    dy = max(max_y - min_y, 0.001)
    pad_x = dx * padding_ratio
    pad_y = dy * padding_ratio
    return f"{min_x - pad_x},{min_y - pad_y},{max_x + pad_x},{max_y + pad_y}"


def wms_url_mapa(ows: str, nombre: str, capa: pd.Series, width: int = 1200, height: int = 900) -> Optional[str]:
    bbox = bbox_expandido(capa)
    if bbox is None:
        return None
    epsg = 4326 if pd.isna(capa.epsg) else int(capa.epsg)
    return (
        f"{ows}?service=wms&version=1.1.0&request=GetMap&layers={nombre}"
        f"&styles=&format=image/png&transparent=false&srs=EPSG:{epsg}"
        f"&width={width}&height={height}&bbox={bbox}"
    )


def wms_url_leyenda(ows: str, nombre: str) -> str:
    return (
        f"{ows}?service=wms&version=1.1.0&request=GetLegendGraphic"
        f"&format=image/png&layer={nombre}"
    )


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

    try:
        data = xmltodict.parse(respuesta.data)
    except Exception:
        return None

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


def getfeature_fields(
    nombre: str,
    *,
    max_features: Optional[int] = None,
    start_index: Optional[int] = None,
) -> dict:
    fields = {
        "service": "wfs",
        "version": "1.0.0",
        "request": "GetFeature",
        "typeName": nombre,
        "outputFormat": "application/json",
    }
    if max_features is not None:
        fields["maxFeatures"] = str(max_features)
        fields["count"] = str(max_features)
    if start_index is not None and start_index > 0:
        fields["startIndex"] = str(start_index)
    return fields


def solicitar_geojson_json(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
    *,
    max_features: Optional[int] = None,
    start_index: Optional[int] = None,
    log_label: Optional[str] = None,
) -> dict:
    respuesta = sesion.request(
        "GET",
        ows,
        fields=getfeature_fields(nombre, max_features=max_features, start_index=start_index),
        preload_content=False,
    )
    if respuesta.status != 200:
        respuesta.release_conn()
        raise RuntimeError(f"GetFeature devolvio estatus {respuesta.status}")

    descargados = 0
    ultimo_reporte = time.monotonic()
    data = bytearray()
    try:
        while True:
            chunk = respuesta.read(STREAM_CHUNK_SIZE)
            if not chunk:
                break
            data.extend(chunk)
            descargados += len(chunk)
            if log_label:
                ahora_ts = time.monotonic()
                if ahora_ts - ultimo_reporte >= DOWNLOAD_HEARTBEAT_SECONDS:
                    print(
                        f"descargando {nombre}: {log_label}, {descargados / (1024 * 1024):.1f} MiB"
                    )
                    ultimo_reporte = ahora_ts
    finally:
        respuesta.release_conn()

    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"GetFeature no devolvio GeoJSON valido para {nombre}") from e


def descargar_geojson_streaming(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
    destino: Path,
) -> int:
    print(f"descargando {nombre}: respuesta unica en streaming")
    respuesta = sesion.request(
        "GET",
        ows,
        fields=getfeature_fields(nombre),
        preload_content=False,
    )
    if respuesta.status != 200:
        raise RuntimeError(f"GetFeature devolvio estatus {respuesta.status}")

    descargados = 0
    ultimo_reporte = time.monotonic()
    destino.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(destino, "wb") as f:
            while True:
                chunk = respuesta.read(STREAM_CHUNK_SIZE)
                if not chunk:
                    break
                f.write(chunk)
                descargados += len(chunk)
                ahora_ts = time.monotonic()
                if ahora_ts - ultimo_reporte >= DOWNLOAD_HEARTBEAT_SECONDS:
                    print(f"descargando {nombre}: {descargados / (1024 * 1024):.1f} MiB")
                    ultimo_reporte = ahora_ts
    finally:
        respuesta.release_conn()

    print(f"descargando {nombre}: completo, {descargados / (1024 * 1024):.1f} MiB")
    return descargados


def feature_fingerprint(feature: dict) -> str:
    return hashlib.sha1(
        json.dumps(feature, sort_keys=True, ensure_ascii=False).encode("utf-8")
    ).hexdigest()


def escribir_feature_collection(destino: Path, features_por_pagina: list[list[dict]]) -> None:
    destino.parent.mkdir(parents=True, exist_ok=True)
    with open(destino, "w", encoding="utf-8") as f:
        f.write('{"type":"FeatureCollection","features":[')
        primero = True
        for features in features_por_pagina:
            for feature in features:
                if not primero:
                    f.write(",")
                json.dump(feature, f, ensure_ascii=False, separators=(",", ":"))
                primero = False
        f.write("]}")


def descargar_geojson_paginado(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
    destino: Path,
    total_esperado: int,
    page_size: int = WFS_PAGE_SIZE,
) -> tuple[int, int]:
    features_por_pagina: list[list[dict]] = []
    total_descargado = 0
    page_index = 0
    primera_huella: Optional[str] = None

    while total_descargado < total_esperado:
        start_index = page_index * page_size
        pagina = solicitar_geojson_json(
            sesion,
            ows,
            nombre,
            max_features=page_size,
            start_index=start_index,
            log_label=f"pagina {page_index + 1}",
        )
        features = pagina.get("features", []) or []
        if page_index == 0 and not features:
            raise RuntimeError("La primera pagina WFS no contiene features")
        if page_index == 1 and features and primera_huella is not None:
            if feature_fingerprint(features[0]) == primera_huella:
                raise RuntimeError("El servidor no parece soportar paginacion startIndex")
        if not features:
            break
        if page_index == 0:
            primera_huella = feature_fingerprint(features[0])
            if len(features) > page_size:
                print(
                    f"aviso: {nombre} ignoro maxFeatures y devolvio {len(features)} features en la primera pagina"
                )

        features_por_pagina.append(features)
        total_descargado += len(features)
        print(
            f"descargando {nombre}: pagina {page_index + 1}, features {total_descargado}/{total_esperado}"
        )

        if len(features) < page_size:
            break
        page_index += 1

    escribir_feature_collection(destino, features_por_pagina)
    return total_descargado, destino.stat().st_size


def descargar_geojson(
    sesion: urllib3.PoolManager,
    ows: str,
    nombre: str,
    destino: Path,
) -> tuple[int, Optional[int], int, bool]:
    total_esperado = consultar_hits(sesion, ows, nombre)

    if total_esperado is not None and total_esperado > WFS_PAGE_SIZE:
        try:
            total_descargado, bytes_descargados = descargar_geojson_paginado(
                sesion,
                ows,
                nombre,
                destino,
                total_esperado=total_esperado,
            )
            if total_descargado == total_esperado:
                return total_descargado, total_esperado, bytes_descargados, True
            print(
                f"aviso: paginacion incompleta para {nombre}: {total_descargado}/{total_esperado}; se reintentara con respuesta unica"
            )
        except Exception as e:
            print(f"aviso: no se pudo paginar {nombre}: {e}; se intentara descarga unica")

    bytes_descargados = descargar_geojson_streaming(sesion, ows, nombre, destino)
    data = json.loads(destino.read_text(encoding="utf-8"))
    total_descargado = len(data.get("features", []) or [])
    if total_esperado is not None:
        print(
            f"descargando {nombre}: features {total_descargado}/{total_esperado}"
        )
    return total_descargado, total_esperado, bytes_descargados, False


def descargar_imagen_opcional(sesion: urllib3.PoolManager, url: str) -> Optional[bytes]:
    try:
        respuesta = sesion.request("GET", url)
    except Exception:
        return None

    if respuesta.status != 200:
        return None

    content_type = (respuesta.headers.get("Content-Type") or "").lower()
    if "image" not in content_type:
        return None

    return bytes(respuesta.data)


def descargar_bytes_archive_org(
    sesion: urllib3.PoolManager,
    item: str,
    filename: str,
) -> Optional[bytes]:
    try:
        respuesta = sesion.request("GET", archive_download_url(item, filename), redirect=False)
    except Exception:
        return None
    if respuesta.status in [301, 302, 303, 307, 308]:
        location = respuesta.headers.get("Location")
        if not location:
            return None
        try:
            respuesta = sesion.request("GET", location)
        except Exception:
            return None
    if respuesta.status != 200:
        return None
    return bytes(respuesta.data)


def metadata_item_archive_org_remoto(
    sesion: urllib3.PoolManager,
    item: str,
) -> Optional[dict]:
    try:
        respuesta = sesion.request("GET", archive_metadata_url(item))
    except Exception:
        return None
    if respuesta.status == 404:
        return None
    if respuesta.status != 200:
        raise RuntimeError(f"Archive.org metadata devolvio estatus {respuesta.status} para {item}")
    data = json.loads(respuesta.data.decode("utf-8"))
    if not data:
        return None
    metadata = data.get("metadata") or {}
    files = data.get("files") or []
    if not metadata and not files:
        return None
    return data


def parse_size_archivo_archive_org(valor) -> Optional[int]:
    if valor in [None, ""]:
        return None
    try:
        return int(valor)
    except Exception:
        return None


def bbox_area_metadata(metadata: dict) -> Optional[float]:
    bbox = metadata.get("bbox") or {}
    try:
        min_x = float(bbox["min_x"])
        max_x = float(bbox["max_x"])
        min_y = float(bbox["min_y"])
        max_y = float(bbox["max_y"])
    except Exception:
        return None
    area = abs((max_x - min_x) * (max_y - min_y))
    if math.isnan(area) or math.isinf(area):
        return None
    return area


def geojson_a_gdf(geojson_path: Path, epsg: Optional[int]) -> gpd.GeoDataFrame:
    gdf = gpd.read_file(geojson_path)
    if gdf.empty:
        raise RuntimeError("El dataset no contiene features")
    if epsg and gdf.crs is None:
        gdf = gdf.set_crs(epsg=int(epsg), allow_override=True)
    return gdf


def muestra_no_geometrica(gdf: gpd.GeoDataFrame) -> dict:
    columnas = [c for c in gdf.columns if c != gdf.geometry.name]
    if not columnas or gdf.empty:
        return {}

    fila = gdf[columnas].iloc[0].to_dict()
    muestra = {}
    for clave, valor in fila.items():
        if pd.isna(valor):
            muestra[clave] = None
        elif hasattr(valor, "item"):
            muestra[clave] = valor.item()
        else:
            muestra[clave] = valor
    return muestra


def descripcion_archive_org(
    fuente_legible: str,
    geoserver: str,
    nombre: str,
    titulo: str,
    descripcion: str,
    fecha_archivado: str,
    epsg: Optional[int],
    capa: pd.Series,
    n_features: int,
    tiene_legend: bool,
) -> str:
    contenido = [
        "dataset.geoparquet",
        "dataset.geojson",
        "metadata.json",
    ]
    if tiene_legend:
        contenido.append("legend.png")
    contenido.append("cover.png")

    epsg_texto = "No disponible" if epsg is None else str(epsg)
    bbox_texto = ", ".join(
        [
            texto_o_vacio(capa.min_x) or "NA",
            texto_o_vacio(capa.min_y) or "NA",
            texto_o_vacio(capa.max_x) or "NA",
            texto_o_vacio(capa.max_y) or "NA",
        ]
    )

    return (
        f"{titulo}\n\n"
        "Dataset geográfico vectorial archivado desde un servicio GeoServer público relacionado con Bolivia.\n\n"
        f"Fuente: {fuente_legible}\n"
        f"Identificador interno de fuente: {geoserver}\n"
        f"Nombre de capa en el servidor: {nombre}\n"
        f"Fecha de archivado: {fecha_archivado}\n\n"
        "Descripción reportada por la fuente:\n"
        f"{descripcion}\n\n"
        "Cobertura espacial:\n"
        f"EPSG: {epsg_texto}\n"
        f"BBox: {bbox_texto}\n"
        f"Número de features: {n_features}\n\n"
        "Contenido del paquete:\n"
        + "\n".join(f"- {item}" for item in contenido)
        + "\n\n"
        + "Este item fue archivado automáticamente por el proyecto GeoDatos para preservar datasets geográficos públicos que pueden dejar de estar disponibles en su fuente original."
    )


def metadata_item_archive_org(
    fuente_legible: str,
    geoserver: str,
    nombre: str,
    titulo: str,
    descripcion: str,
    fecha_archivado: str,
    ows: str,
    epsg: Optional[int],
    capa: pd.Series,
    n_features: int,
    tiene_legend: bool,
) -> dict:
    return {
        "title": f"{titulo} - {fuente_legible}",
        "creator": fuente_legible,
        "mediatype": "data",
        "collection": os.environ.get("ARCHIVE_COLLECTION", "opensource"),
        "date": fecha_archivado,
        "source": ows,
        "language": "spa",
        "subject": [
            "Bolivia",
            "Geospatial",
            "Vector data",
            fuente_legible,
            geoserver,
            nombre,
            "GeoJSON",
            "GeoParquet",
        ],
        "description": descripcion_archive_org(
            fuente_legible=fuente_legible,
            geoserver=geoserver,
            nombre=nombre,
            titulo=titulo,
            descripcion=descripcion,
            fecha_archivado=fecha_archivado,
            epsg=epsg,
            capa=capa,
            n_features=n_features,
            tiene_legend=tiene_legend,
        ),
    }


def metadata_json(
    geoserver: str,
    nombre: str,
    archive_item: str,
    fecha_archivado: str,
    capa: pd.Series,
    n_features: int,
    fuente_legible: str,
    ows: str,
) -> dict:
    titulo = titulo_legible(capa)
    descripcion = texto_o_vacio(capa.descripcion)
    return {
        "geoserver": geoserver,
        "fuente": fuente_legible,
        "ows": ows,
        "nombre": nombre,
        "titulo": titulo,
        "descripcion": descripcion,
        "fecha_archivado": fecha_archivado,
        "archive_item": archive_item,
        "epsg": None if pd.isna(capa.epsg) else int(capa.epsg),
        "bbox": {
            "min_x": None if pd.isna(capa.min_x) else float(capa.min_x),
            "max_x": None if pd.isna(capa.max_x) else float(capa.max_x),
            "min_y": None if pd.isna(capa.min_y) else float(capa.min_y),
            "max_y": None if pd.isna(capa.max_y) else float(capa.max_y),
        },
        "n_features": int(n_features),
    }


def escribir_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def generar_mapa_publicacion(
    geoserver: str,
    nombre: str,
    destino: Path,
    map_mode: str,
    sesion: urllib3.PoolManager,
    ows: str,
    capa: pd.Series,
) -> None:
    if map_mode == "none":
        return
    if map_mode == "wms":
        try:
            map_url = wms_url_mapa(ows, nombre, capa)
            if map_url:
                map_bytes = descargar_imagen_opcional(sesion, map_url)
                if map_bytes:
                    destino.write_bytes(map_bytes)
        except Exception as e:
            print(f"aviso: no se pudo generar map.png para {geoserver} / {nombre}: {e}")
        return
    if map_mode == "composed":
        try:
            componer_mapa(
                geoserver=geoserver,
                nombre=nombre,
                output=str(destino),
            )
        except Exception as e:
            print(f"aviso: no se pudo generar map.png para {geoserver} / {nombre}: {e}")
        return
    raise ValueError(f"map_mode desconocido: {map_mode}")


def stage_dataset(
    sesion: urllib3.PoolManager,
    directorio: Dict[str, dict],
    capa: pd.Series,
    map_mode: str,
) -> dict:
    geoserver = capa.geoserver
    nombre = capa.nombre
    entrada = directorio.get(geoserver)
    if not entrada:
        raise KeyError(f"No se encontro geoserver {geoserver} en directorio.json")

    ows = entrada["ows"]
    fuente_legible = entrada.get("descripcion") or geoserver
    fecha_archivado = hoy()
    archive_item = item_name(geoserver, nombre)

    with tempfile.TemporaryDirectory(prefix="geodatos-archivar-") as tmpdir:
        tmp = Path(tmpdir)

        geojson_path = tmp / "dataset.geojson"
        geoparquet_path = tmp / "dataset.geoparquet"
        metadata_path = tmp / "metadata.json"
        legend_path = tmp / "legend.png"
        map_path = tmp / "map.png"
        sample_path = tmp / "sample.json"

        n_features_descargadas, n_features_esperadas, bytes_geojson, paginado = descargar_geojson(
            sesion,
            ows,
            nombre,
            geojson_path,
        )

        gdf = geojson_a_gdf(geojson_path, capa.epsg if not pd.isna(capa.epsg) else None)
        gdf.to_parquet(geoparquet_path)

        n_features = len(gdf)
        if n_features_esperadas is not None and n_features != n_features_esperadas:
            print(
                f"aviso: {nombre} descargo {n_features} features pero hits reporta {n_features_esperadas}"
            )
        if paginado:
            print(f"descargando {nombre}: paginacion completada con {n_features_descargadas} features")
        escribir_json(sample_path, muestra_no_geometrica(gdf))

        legend_bytes = descargar_imagen_opcional(sesion, wms_url_leyenda(ows, nombre))
        if legend_bytes:
            legend_path.write_bytes(legend_bytes)

        metadata = metadata_json(
            geoserver=geoserver,
            nombre=nombre,
            archive_item=archive_item,
            fecha_archivado=fecha_archivado,
            capa=capa,
            n_features=n_features,
            fuente_legible=fuente_legible,
            ows=ows,
        )
        escribir_json(metadata_path, metadata)

        generar_mapa_publicacion(
            geoserver=geoserver,
            nombre=nombre,
            destino=map_path,
            map_mode=map_mode,
            sesion=sesion,
            ows=ows,
            capa=capa,
        )

        staged = tmp / "staged"
        staged.mkdir()
        for path in [geojson_path, geoparquet_path, metadata_path, sample_path]:
            shutil.copy2(path, staged / path.name)
        if legend_path.exists():
            shutil.copy2(legend_path, staged / legend_path.name)
        if map_path.exists():
            shutil.copy2(map_path, staged / "cover.png")

        persisted = TMP_STAGE / archive_item
        if persisted.exists():
            shutil.rmtree(persisted)
        persisted.parent.mkdir(parents=True, exist_ok=True)
        shutil.copytree(staged, persisted)

        return {
        "archive_item": archive_item,
        "fecha_archivado": fecha_archivado,
        "staged": persisted,
        "metadata_path": persisted / "metadata.json",
        "geojson_path": persisted / "dataset.geojson",
        "geoparquet_path": persisted / "dataset.geoparquet",
        "legend_path": persisted / "legend.png",
        "map_path": persisted / "cover.png",
        "sample_path": persisted / "sample.json",
        "archive_metadata": metadata_item_archive_org(
            fuente_legible=fuente_legible,
            geoserver=geoserver,
            nombre=nombre,
            titulo=titulo_legible(capa),
            descripcion=descripcion_legible(capa),
            fecha_archivado=fecha_archivado,
            ows=ows,
            epsg=None if pd.isna(capa.epsg) else int(capa.epsg),
            capa=capa,
            n_features=n_features,
            tiene_legend=(persisted / "legend.png").exists(),
        ),
        "bytes_geojson": bytes_geojson,
    }


def subir_archive_org(staged: dict) -> None:
    try:
        from internetarchive import upload
    except ImportError as e:
        raise RuntimeError(
            "Falta la dependencia internetarchive. Instala requirements.txt antes de ejecutar archivar."
        ) from e

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Faltan IA_ACCESS_KEY y/o IA_SECRET_KEY en el entorno")

    archivos = [
        str(staged["geojson_path"]),
        str(staged["geoparquet_path"]),
        str(staged["metadata_path"]),
    ]
    if staged["legend_path"].exists():
        archivos.append(str(staged["legend_path"]))
    if staged["map_path"].exists():
        archivos.append(str(staged["map_path"]))

    resultado = upload(
        staged["archive_item"],
        archivos,
        metadata=staged["archive_metadata"],
        access_key=access_key,
        secret_key=secret_key,
        verify=False,
        verbose=True,
    )

    if isinstance(resultado, Iterable):
        ok = all(r.status_code in [200, 302] for r in resultado)
        if not ok:
            raise RuntimeError("La subida a Archive.org no completo correctamente")


def publicar_local(staged: dict, geoserver: str, nombre: str) -> Dict[str, bool]:
    rutas = rutas_publicacion(geoserver, nombre)
    rutas["base"].mkdir(parents=True, exist_ok=True)
    shutil.copy2(staged["metadata_path"], rutas["metadata"])

    tiene_legend = staged["legend_path"].exists()
    if tiene_legend:
        shutil.copy2(staged["legend_path"], rutas["legend"])
    elif rutas["legend"].exists():
        rutas["legend"].unlink()

    tiene_map = staged["map_path"].exists()
    if tiene_map:
        shutil.copy2(staged["map_path"], rutas["map"])
    elif rutas["map"].exists():
        rutas["map"].unlink()

    tiene_sample = staged["sample_path"].exists()
    if tiene_sample:
        shutil.copy2(staged["sample_path"], rutas["sample"])
    elif rutas["sample"].exists():
        rutas["sample"].unlink()

    return {
        "tiene_legend_png": tiene_legend,
        "tiene_map_png": tiene_map,
        "tiene_sample_json": tiene_sample,
    }


def publicar_local_desde_archive_org(
    sesion: urllib3.PoolManager,
    item: str,
    geoserver: str,
    nombre: str,
) -> Dict[str, bool]:
    rutas = rutas_publicacion(geoserver, nombre)
    rutas["base"].mkdir(parents=True, exist_ok=True)

    metadata_bytes = descargar_bytes_archive_org(sesion, item, "metadata.json")
    if metadata_bytes is None:
        raise RuntimeError(f"No se pudo descargar metadata.json desde Archive.org para {item}")
    rutas["metadata"].write_bytes(metadata_bytes)

    legend_bytes = descargar_bytes_archive_org(sesion, item, "legend.png")
    if legend_bytes:
        rutas["legend"].write_bytes(legend_bytes)
    elif rutas["legend"].exists():
        rutas["legend"].unlink()

    map_bytes = (
        descargar_bytes_archive_org(sesion, item, "cover.png")
        or descargar_bytes_archive_org(sesion, item, "map.png")
    )
    if map_bytes:
        rutas["map"].write_bytes(map_bytes)
    elif rutas["map"].exists():
        rutas["map"].unlink()

    sample_bytes = descargar_bytes_archive_org(sesion, item, "sample.json")
    if sample_bytes:
        rutas["sample"].write_bytes(sample_bytes)
    elif rutas["sample"].exists():
        rutas["sample"].unlink()

    return {
        "tiene_legend_png": bool(legend_bytes),
        "tiene_map_png": bool(map_bytes),
        "tiene_sample_json": bool(sample_bytes),
    }


def fila_paquete_desde_archive_org(
    archive_item: str,
    remote_item: dict,
    metadata: dict,
    publicados: Dict[str, bool],
) -> dict:
    archivos = {archivo.get("name"): archivo for archivo in remote_item.get("files", [])}
    return {
        "geoserver": metadata.get("geoserver", ""),
        "nombre": metadata.get("nombre", ""),
        "titulo": metadata.get("titulo", ""),
        "descripcion": metadata.get("descripcion", "") or "",
        "fecha_archivado": metadata.get("fecha_archivado", ""),
        "archive_item": archive_item,
        "epsg": metadata.get("epsg", "") if metadata.get("epsg", None) is not None else "",
        "bbox_area": "" if bbox_area_metadata(metadata) is None else bbox_area_metadata(metadata),
        "n_features": metadata.get("n_features", ""),
        "bytes_geojson": parse_size_archivo_archive_org((archivos.get("dataset.geojson") or {}).get("size")) or "",
        "bytes_geoparquet": parse_size_archivo_archive_org((archivos.get("dataset.geoparquet") or {}).get("size")) or "",
        **publicados,
    }


def actualizar_paquetes_desde_fila(paquetes: pd.DataFrame, fila: dict) -> pd.DataFrame:
    paquetes = paquetes[
        ~((paquetes["geoserver"] == fila["geoserver"]) & (paquetes["nombre"] == fila["nombre"]))
    ].copy()
    if paquetes.empty:
        paquetes = pd.DataFrame([fila])
    else:
        paquetes = pd.concat([paquetes, pd.DataFrame([fila])], ignore_index=True)
    paquetes = paquetes.sort_values(["geoserver", "nombre"], kind="mergesort").reset_index(drop=True)
    return paquetes[PAQUETES_COLUMNAS]


def sincronizar_item_archive_org(
    sesion: urllib3.PoolManager,
    datasets: pd.DataFrame,
    paquetes: pd.DataFrame,
    geoserver: str,
    nombre: str,
) -> tuple[bool, pd.DataFrame, pd.DataFrame]:
    archive_item = item_name(geoserver, nombre)
    remote_item = metadata_item_archive_org_remoto(sesion, archive_item)
    if remote_item is None:
        return False, datasets, paquetes

    archivos = {archivo.get("name") for archivo in remote_item.get("files", [])}
    requeridos = {"dataset.geojson", "dataset.geoparquet", "metadata.json"}
    if not requeridos.issubset(archivos):
        print(
            f"aviso: {archive_item} existe en Archive.org pero esta incompleto; se intentara reparar con una nueva subida"
        )
        return False, datasets, paquetes

    metadata_bytes = descargar_bytes_archive_org(sesion, archive_item, "metadata.json")
    if metadata_bytes is None:
        print(
            f"aviso: no se pudo descargar metadata.json para {archive_item}; se intentara reparar con una nueva subida"
        )
        return False, datasets, paquetes
    metadata = json.loads(metadata_bytes.decode("utf-8"))

    publicados = publicar_local_desde_archive_org(sesion, archive_item, geoserver, nombre)
    fila = fila_paquete_desde_archive_org(archive_item, remote_item, metadata, publicados)
    paquetes = actualizar_paquetes_desde_fila(paquetes, fila)
    fecha_archivado = texto_o_vacio(metadata.get("fecha_archivado")) or hoy()
    datasets = actualizar_datasets_exito(datasets, geoserver, nombre, fecha_archivado)
    return True, datasets, paquetes


def actualizar_paquetes(paquetes: pd.DataFrame, capa: pd.Series, staged: dict, publicados: Dict[str, bool]) -> pd.DataFrame:
    fila = {
        "geoserver": capa.geoserver,
        "nombre": capa.nombre,
        "titulo": capa.titulo,
        "descripcion": capa.descripcion or "",
        "fecha_archivado": staged["fecha_archivado"],
        "archive_item": staged["archive_item"],
        "epsg": "" if pd.isna(capa.epsg) else int(capa.epsg),
        "bbox_area": "" if bbox_area(capa) is None else bbox_area(capa),
        "n_features": len(gpd.read_parquet(staged["geoparquet_path"])),
        "bytes_geojson": staged["geojson_path"].stat().st_size,
        "bytes_geoparquet": staged["geoparquet_path"].stat().st_size,
        **publicados,
    }

    paquetes = paquetes[
        ~((paquetes["geoserver"] == capa.geoserver) & (paquetes["nombre"] == capa.nombre))
    ].copy()
    if paquetes.empty:
        paquetes = pd.DataFrame([fila])
    else:
        paquetes = pd.concat([paquetes, pd.DataFrame([fila])], ignore_index=True)
    paquetes = paquetes.sort_values(["geoserver", "nombre"], kind="mergesort").reset_index(drop=True)
    return paquetes[PAQUETES_COLUMNAS]


def guardar_paquetes(paquetes: pd.DataFrame) -> None:
    salida = paquetes.copy()
    for columna in ["tiene_legend_png", "tiene_map_png", "tiene_sample_json"]:
        salida[columna] = salida[columna].astype(bool)
    salida.to_csv(PAQUETES, index=False, float_format="%.6f")


def actualizar_datasets_exito(datasets: pd.DataFrame, geoserver: str, nombre: str, fecha: str) -> pd.DataFrame:
    mascara = (datasets["geoserver"] == geoserver) & (datasets["nombre"] == nombre)
    datasets.loc[mascara, "archivado"] = True
    datasets.loc[mascara, "fecha_ultimo_archivo_intento"] = fecha
    datasets.loc[mascara, "fecha_archivado"] = fecha
    return datasets


def actualizar_datasets_error(datasets: pd.DataFrame, geoserver: str, nombre: str, fecha: str) -> pd.DataFrame:
    mascara = (datasets["geoserver"] == geoserver) & (datasets["nombre"] == nombre)
    datasets.loc[mascara, "errores_archivar"] = (
        pd.to_numeric(datasets.loc[mascara, "errores_archivar"], errors="coerce").fillna(0) + 1
    )
    datasets.loc[mascara, "fecha_ultimo_archivo_intento"] = fecha
    return datasets


def ordenar_datasets(datasets: pd.DataFrame) -> pd.DataFrame:
    ordenado = datasets.copy()
    ordenado["wfs_activo"] = ordenado["wfs_activo"].astype(str).eq("True")
    ordenado["archivado"] = ordenado["archivado"].astype(str).eq("True")
    ordenado["tiene_descripcion"] = ordenado["descripcion"].astype(str).str.strip().ne("")
    ordenado["fallas_wfs_90d"] = pd.to_numeric(ordenado["fallas_wfs_90d"], errors="coerce").fillna(0)
    ordenado["errores_totales"] = (
        pd.to_numeric(ordenado["errores_evaluar"], errors="coerce").fillna(0)
        + pd.to_numeric(ordenado["errores_archivar"], errors="coerce").fillna(0)
    )
    ordenado["bytes_estimados_orden"] = pd.to_numeric(ordenado["bytes_estimados"], errors="coerce").fillna(float("inf"))
    ordenado["bbox_area_orden"] = pd.to_numeric(ordenado["bbox_area"], errors="coerce").fillna(-1.0)
    ordenado["n_features_orden"] = pd.to_numeric(ordenado["n_features"], errors="coerce").fillna(-1.0)

    ordenado = ordenado.sort_values(
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

    return ordenado.drop(
        columns=[
            "tiene_descripcion",
            "errores_totales",
            "bytes_estimados_orden",
            "bbox_area_orden",
            "n_features_orden",
        ]
    )


def guardar_datasets(datasets: pd.DataFrame) -> None:
    salida = ordenar_datasets(datasets)
    for columna in ["wfs_activo", "archivado"]:
        salida[columna] = salida[columna].astype(bool)
    for columna in ["errores_evaluar", "errores_archivar", "fallas_wfs_90d"]:
        if columna in salida.columns:
            salida[columna] = pd.to_numeric(salida[columna], errors="coerce").fillna(0).astype(int)
    salida.to_csv(DATASETS, index=False, float_format="%.6f")


def registrar_run(runs: pd.DataFrame, geoserver: str, nombre: str, fecha_inicio: pd.Timestamp, fecha_fin: pd.Timestamp, estado: str, detalle: str) -> pd.DataFrame:
    fila = {
        "geoserver": geoserver,
        "nombre": nombre,
        "fecha_inicio": fecha_inicio.isoformat(timespec="seconds"),
        "fecha_fin": fecha_fin.isoformat(timespec="seconds"),
        "segundos": round((fecha_fin - fecha_inicio).total_seconds(), 3),
        "estado": estado,
        "detalle": detalle,
    }
    return pd.concat([runs, pd.DataFrame([fila])], ignore_index=True)[RUNS_COLUMNAS]


def registrar_run_inicio(runs: pd.DataFrame, geoserver: str, nombre: str, fecha_inicio: pd.Timestamp) -> pd.DataFrame:
    fila = {
        "geoserver": geoserver,
        "nombre": nombre,
        "fecha_inicio": fecha_inicio.isoformat(timespec="seconds"),
        "fecha_fin": "",
        "segundos": "",
        "estado": "running",
        "detalle": "",
    }
    return pd.concat([runs, pd.DataFrame([fila])], ignore_index=True)[RUNS_COLUMNAS]


def guardar_runs(runs: pd.DataFrame) -> None:
    runs.to_csv(RUNS, index=False)


def limpiar_stage(staged: dict) -> None:
    base = staged.get("staged")
    if base and Path(base).exists():
        shutil.rmtree(base)


def timeout_handler_factory(geoserver: str, nombre: str, max_seconds: int):
    def handler(signum, frame):
        raise DatasetTimeoutError(
            f"Timeout archivando {geoserver} / {nombre} despues de {max_seconds // 60} minutos"
        )

    return handler


def archivar_uno(
    sesion: urllib3.PoolManager,
    directorio: Dict[str, dict],
    capas: pd.DataFrame,
    datasets: pd.DataFrame,
    paquetes: pd.DataFrame,
    runs: pd.DataFrame,
    geoserver: str,
    nombre: str,
    dry_run: bool,
    map_mode: str,
    max_minutes_por_dataset: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    fecha_inicio = ahora()
    fecha = fecha_inicio.date().isoformat()
    staged = None
    datasets_original = datasets.copy()
    paquetes_original = paquetes.copy()
    runs_original = runs.copy()
    timeout_seconds = max(60, int(max_minutes_por_dataset * 60))
    previous_handler = signal.getsignal(signal.SIGALRM)

    try:
        signal.signal(
            signal.SIGALRM,
            timeout_handler_factory(geoserver, nombre, timeout_seconds),
        )
        signal.alarm(timeout_seconds)

        mascara = (datasets["geoserver"] == geoserver) & (datasets["nombre"] == nombre)
        runs = registrar_run_inicio(runs, geoserver, nombre, fecha_inicio)
        datasets.loc[mascara, "fecha_ultimo_archivo_intento"] = fecha
        if not dry_run:
            guardar_runs(runs)
            guardar_datasets(datasets)

        sincronizado, datasets, paquetes = sincronizar_item_archive_org(
            sesion=sesion,
            datasets=datasets,
            paquetes=paquetes,
            geoserver=geoserver,
            nombre=nombre,
        )
        if sincronizado:
            runs = registrar_run(runs, geoserver, nombre, fecha_inicio, ahora(), "ok", "synced_from_archive_org")
            if dry_run:
                runs = runs_original
                datasets = datasets_original
                paquetes = paquetes_original
            else:
                guardar_paquetes(paquetes)
                guardar_datasets(datasets)
                guardar_runs(runs)
            return datasets, paquetes, runs

        capa = seleccionar_capa(capas, geoserver, nombre)
        staged = stage_dataset(sesion, directorio, capa, map_mode=map_mode)
        if not dry_run:
            subir_archive_org(staged)
        publicados = publicar_local(staged, geoserver, nombre)
        paquetes = actualizar_paquetes(paquetes, capa, staged, publicados)
        datasets = actualizar_datasets_exito(datasets, geoserver, nombre, fecha)
        runs = registrar_run(runs, geoserver, nombre, fecha_inicio, ahora(), "ok", "")
        if dry_run:
            runs = runs_original
            datasets = datasets_original
            paquetes = paquetes_original
        else:
            guardar_paquetes(paquetes)
            guardar_datasets(datasets)
            guardar_runs(runs)
        return datasets, paquetes, runs
    except Exception as e:
        datasets = actualizar_datasets_error(datasets, geoserver, nombre, fecha)
        runs = registrar_run(runs, geoserver, nombre, fecha_inicio, ahora(), "error", str(e))
        if dry_run:
            runs = runs_original
            datasets = datasets_original
            paquetes = paquetes_original
        else:
            guardar_datasets(datasets)
            guardar_runs(runs)
        raise
    finally:
        if staged is not None:
            limpiar_stage(staged)
        if dry_run:
            rutas = rutas_publicacion(geoserver, nombre)
            if rutas["base"].exists():
                shutil.rmtree(rutas["base"], ignore_errors=True)
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous_handler)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-datasets", type=int, default=1)
    parser.add_argument("--max-minutes", type=int, default=330)
    parser.add_argument("--max-minutes-por-dataset", type=int, default=25)
    parser.add_argument("--max-errores-archivar", type=int, default=MAX_ERRORES_ARCHIVAR)
    parser.add_argument("--map-mode", choices=["none", "wms", "composed"], default="composed")
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    limpiar_stage_huerfano()

    datasets = leer_datasets()
    capas = leer_capas()
    directorio = leer_directorio()
    blacklist = leer_blacklist()
    paquetes = leer_paquetes()
    runs = leer_runs()

    candidatos = seleccionar_candidatos(
        datasets,
        geoservers_excluidos=blacklist,
        max_errores_archivar=args.max_errores_archivar,
    )
    if candidatos.empty:
        print("No hay datasets candidatos para archivar.")
        return 2

    sesion = iniciar_sesion()
    inicio = time.monotonic()
    procesados = 0

    for fila in candidatos.itertuples(index=False):
        minutos = (time.monotonic() - inicio) / 60
        if procesados >= args.max_datasets or minutos >= args.max_minutes:
            break

        print(f"archivando {fila.geoserver} / {fila.nombre}")
        datasets, paquetes, runs = archivar_uno(
            sesion=sesion,
            directorio=directorio,
            capas=capas,
            datasets=datasets,
            paquetes=paquetes,
            runs=runs,
            geoserver=fila.geoserver,
            nombre=fila.nombre,
            dry_run=args.dry_run,
            map_mode=args.map_mode,
            max_minutes_por_dataset=args.max_minutes_por_dataset,
        )
        procesados += 1

    print(f"datasets archivados en esta corrida: {procesados}")
    if procesados == 0:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

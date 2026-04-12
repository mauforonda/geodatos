#!/usr/bin/env python3

import argparse
import io
import json
import math
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from PIL import Image
from pyproj import Transformer


ROOT_DIR = Path(__file__).resolve().parent.parent
DESCUBRIR_DIR = ROOT_DIR / "descubrir"
PUBLICADOS_DIR = ROOT_DIR / "archivar" / "publicados"

CAPAS = DESCUBRIR_DIR / "capas.csv"
DIRECTORIO = DESCUBRIR_DIR / "directorio.json"

TILE_SIZE = 256
MERCATOR_MAX = 20037508.342789244
WEBMERCATOR_EPSG = 3857
USER_AGENT = "geodatos-map-composer/1.0"


def slug(texto: str) -> str:
    import re

    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", texto.strip())).strip("_").lower()


def leer_capa(geoserver: str, nombre: str) -> pd.Series:
    capas = pd.read_csv(CAPAS, keep_default_na=False)
    fila = capas[(capas["geoserver"] == geoserver) & (capas["nombre"] == nombre)]
    if fila.empty:
        raise KeyError(f"No se encontro {geoserver} / {nombre} en descubrir/capas.csv")
    return fila.iloc[0]


def leer_ows(geoserver: str) -> str:
    with open(DIRECTORIO, "r") as f:
        directorio = json.load(f)
    for entrada in directorio:
        if entrada["nombre"] == geoserver:
            return entrada["ows"]
    raise KeyError(f"No se encontro {geoserver} en descubrir/directorio.json")


def expandir_bbox(min_x: float, min_y: float, max_x: float, max_y: float, padding_ratio: float = 0.15) -> tuple[float, float, float, float]:
    dx = max(max_x - min_x, 0.001)
    dy = max(max_y - min_y, 0.001)
    pad_x = dx * padding_ratio
    pad_y = dy * padding_ratio
    return min_x - pad_x, min_y - pad_y, max_x + pad_x, max_y + pad_y


def lonlat_to_webmercator(lon: float, lat: float) -> tuple[float, float]:
    transformer = Transformer.from_crs(4326, WEBMERCATOR_EPSG, always_xy=True)
    return transformer.transform(lon, lat)


def dimensiones_objetivo(
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    width: int,
    height: Optional[int],
) -> tuple[int, int]:
    if height is not None:
        return width, height

    wm_min_x, wm_min_y = lonlat_to_webmercator(min_x, min_y)
    wm_max_x, wm_max_y = lonlat_to_webmercator(max_x, max_y)
    wm_width = max(abs(wm_max_x - wm_min_x), 1.0)
    wm_height = max(abs(wm_max_y - wm_min_y), 1.0)
    aspect_ratio = wm_width / wm_height
    derived_height = max(1, int(round(width / aspect_ratio)))
    return width, derived_height


def ajustar_bbox_mercator_al_aspect_ratio(
    min_x: float,
    min_y: float,
    max_x: float,
    max_y: float,
    width: int,
    height: int,
) -> tuple[float, float, float, float]:
    bbox_width = max(max_x - min_x, 1.0)
    bbox_height = max(max_y - min_y, 1.0)
    bbox_ratio = bbox_width / bbox_height
    target_ratio = width / height

    cx = (min_x + max_x) / 2
    cy = (min_y + max_y) / 2

    if bbox_ratio < target_ratio:
        bbox_width = bbox_height * target_ratio
    else:
        bbox_height = bbox_width / target_ratio

    half_w = bbox_width / 2
    half_h = bbox_height / 2
    return cx - half_w, cy - half_h, cx + half_w, cy + half_h


def webmercator_to_world_pixels(x: float, y: float, zoom: int) -> tuple[float, float]:
    scale = TILE_SIZE * (2**zoom)
    px = (x + MERCATOR_MAX) / (2 * MERCATOR_MAX) * scale
    py = (MERCATOR_MAX - y) / (2 * MERCATOR_MAX) * scale
    return px, py


def zoom_para_bbox(min_x: float, min_y: float, max_x: float, max_y: float, width: int, height: int, max_zoom: int = 18) -> int:
    x1, y1 = lonlat_to_webmercator(min_x, min_y)
    x2, y2 = lonlat_to_webmercator(max_x, max_y)

    for zoom in range(max_zoom, -1, -1):
        px1, py1 = webmercator_to_world_pixels(x1, y1, zoom)
        px2, py2 = webmercator_to_world_pixels(x2, y2, zoom)
        if abs(px2 - px1) <= width and abs(py2 - py1) <= height:
            return zoom
    return 0


def tile_range_for_bbox(min_x: float, min_y: float, max_x: float, max_y: float, zoom: int) -> tuple[int, int, int, int]:
    x1, y1 = lonlat_to_webmercator(min_x, max_y)
    x2, y2 = lonlat_to_webmercator(max_x, min_y)
    px1, py1 = webmercator_to_world_pixels(x1, y1, zoom)
    px2, py2 = webmercator_to_world_pixels(x2, y2, zoom)
    return (
        int(math.floor(px1 / TILE_SIZE)),
        int(math.floor(py1 / TILE_SIZE)),
        int(math.floor(px2 / TILE_SIZE)),
        int(math.floor(py2 / TILE_SIZE)),
    )


def descargar_tile(session: requests.Session, z: int, x: int, y: int) -> Image.Image:
    subdomain = ["a", "b", "c"][(x + y) % 3]
    url = f"https://{subdomain}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}.png"
    response = session.get(url, timeout=(10, 20))
    response.raise_for_status()
    return Image.open(io.BytesIO(response.content)).convert("RGBA")


def render_basemap(min_x: float, min_y: float, max_x: float, max_y: float, width: int, height: int) -> tuple[Image.Image, int, tuple[float, float, float, float]]:
    wm_min_x, wm_min_y = lonlat_to_webmercator(min_x, min_y)
    wm_max_x, wm_max_y = lonlat_to_webmercator(max_x, max_y)
    wm_min_x, wm_min_y, wm_max_x, wm_max_y = ajustar_bbox_mercator_al_aspect_ratio(
        wm_min_x, wm_min_y, wm_max_x, wm_max_y, width, height
    )

    transformer = Transformer.from_crs(WEBMERCATOR_EPSG, 4326, always_xy=True)
    min_lon, min_lat = transformer.transform(wm_min_x, wm_min_y)
    max_lon, max_lat = transformer.transform(wm_max_x, wm_max_y)

    zoom = zoom_para_bbox(min_lon, min_lat, max_lon, max_lat, width, height)
    tx_min, ty_min, tx_max, ty_max = tile_range_for_bbox(min_lon, min_lat, max_lon, max_lat, zoom)

    canvas = Image.new("RGBA", ((tx_max - tx_min + 1) * TILE_SIZE, (ty_max - ty_min + 1) * TILE_SIZE))
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    for tx in range(tx_min, tx_max + 1):
        for ty in range(ty_min, ty_max + 1):
            tile = descargar_tile(session, zoom, tx, ty)
            canvas.paste(tile, ((tx - tx_min) * TILE_SIZE, (ty - ty_min) * TILE_SIZE))

    px1, py1 = webmercator_to_world_pixels(wm_min_x, wm_max_y, zoom)
    px2, py2 = webmercator_to_world_pixels(wm_max_x, wm_min_y, zoom)

    left = int(round(px1 - tx_min * TILE_SIZE))
    top = int(round(py1 - ty_min * TILE_SIZE))
    right = int(round(px2 - tx_min * TILE_SIZE))
    bottom = int(round(py2 - ty_min * TILE_SIZE))

    crop = canvas.crop((left, top, right, bottom)).resize((width, height), Image.Resampling.LANCZOS)
    return crop, zoom, (wm_min_x, wm_min_y, wm_max_x, wm_max_y)


def descargar_overlay_wms(ows: str, nombre: str, bbox_3857: tuple[float, float, float, float], width: int, height: int) -> Optional[Image.Image]:
    params = {
        "service": "wms",
        "version": "1.1.0",
        "request": "GetMap",
        "layers": nombre,
        "styles": "",
        "format": "image/png",
        "transparent": "true",
        "srs": "EPSG:3857",
        "width": str(width),
        "height": str(height),
        "bbox": ",".join(f"{v:.6f}" for v in bbox_3857),
    }
    response = requests.get(ows, params=params, timeout=(15, 40), verify=False, headers={"User-Agent": USER_AGENT})
    if response.status_code != 200:
        return None
    content_type = (response.headers.get("Content-Type") or "").lower()
    if "image" not in content_type:
        return None
    return Image.open(io.BytesIO(response.content)).convert("RGBA")


def output_path(geoserver: str, nombre: str, override: Optional[str]) -> Path:
    if override:
        return Path(override)
    return PUBLICADOS_DIR / slug(geoserver) / slug(nombre) / "map.png"


def componer_mapa(
    geoserver: str,
    nombre: str,
    width: int = 400,
    height: Optional[int] = None,
    padding_ratio: float = 0.15,
    output: Optional[str] = None,
) -> Path:
    requests.packages.urllib3.disable_warnings()  # type: ignore[attr-defined]

    capa = leer_capa(geoserver, nombre)
    ows = leer_ows(geoserver)

    min_x, min_y, max_x, max_y = expandir_bbox(
        float(capa.min_x),
        float(capa.min_y),
        float(capa.max_x),
        float(capa.max_y),
        padding_ratio=padding_ratio,
    )

    width, height = dimensiones_objetivo(min_x, min_y, max_x, max_y, width, height)

    base, zoom, bbox_3857 = render_basemap(min_x, min_y, max_x, max_y, width, height)
    overlay = descargar_overlay_wms(ows, nombre, bbox_3857, width, height)
    if overlay is None:
        raise RuntimeError("No se pudo obtener el overlay WMS en EPSG:3857")

    compuesto = Image.alpha_composite(base, overlay)
    destino = output_path(geoserver, nombre, output)
    destino.parent.mkdir(parents=True, exist_ok=True)
    compuesto.save(destino)
    print(f"mapa compuesto guardado en {destino}")
    print(f"zoom={zoom}")
    return destino


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geoserver", required=True)
    parser.add_argument("--nombre", required=True)
    parser.add_argument("--width", type=int, default=400)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--padding-ratio", type=float, default=0.15)
    parser.add_argument("--output", default=None)
    args = parser.parse_args()

    componer_mapa(
        geoserver=args.geoserver,
        nombre=args.nombre,
        width=args.width,
        height=args.height,
        padding_ratio=args.padding_ratio,
        output=args.output,
    )


if __name__ == "__main__":
    main()

#!/usr/bin/env python3

import argparse
import json
import sys
import tempfile
import time
from pathlib import Path
from typing import Iterable

import pyogrio
import requests
from PIL import Image, ImageColor, ImageDraw
from pyproj import Transformer
from shapely.geometry import (
    GeometryCollection,
    LineString,
    MultiLineString,
    MultiPoint,
    MultiPolygon,
    Point,
    Polygon,
)
from shapely.geometry.base import BaseGeometry
from shapely.ops import transform as shapely_transform

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from archivar.componer_mapa import (
    USER_AGENT,
    dimensiones_objetivo,
    expandir_bbox,
    normalizar_bbox_lonlat,
    render_basemap,
)


class InvalidGeoJSONArchiveError(RuntimeError):
    pass


def descargar_geojson(url: str, destino: Path) -> Path:
    destino.parent.mkdir(parents=True, exist_ok=True)
    last_error = None
    for attempt in range(1, 5):
        try:
            with requests.get(url, stream=True, timeout=(20, 120), headers={"User-Agent": USER_AGENT}) as response:
                response.raise_for_status()
                total = int(response.headers.get("Content-Length") or 0)
                descargados = 0
                siguiente_reporte = 5 * 1024 * 1024
                with open(destino, "wb") as f:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if chunk:
                            f.write(chunk)
                            descargados += len(chunk)
                            if descargados >= siguiente_reporte:
                                if total:
                                    print(
                                        f"descargando geojson: {descargados / (1024 * 1024):.1f} / {total / (1024 * 1024):.1f} MiB"
                                    )
                                else:
                                    print(
                                        f"descargando geojson: {descargados / (1024 * 1024):.1f} MiB"
                                    )
                                siguiente_reporte += 5 * 1024 * 1024
            validar_geojson_basico(destino)
            return destino
        except InvalidGeoJSONArchiveError:
            raise
        except requests.RequestException as e:
            last_error = e
            if attempt >= 4:
                raise
            espera = attempt * 3
            print(f"aviso: fallo descargando geojson ({e}), reintentando en {espera}s [{attempt}/4]")
            time.sleep(espera)
    if last_error is not None:
        raise last_error
    return destino


def validar_geojson_basico(path: Path) -> None:
    snippet = path.read_text(encoding="utf-8", errors="ignore")[:4096].lstrip("\ufeff \t\r\n")
    if not snippet:
        raise InvalidGeoJSONArchiveError("archivo descargado vacío")
    if snippet.startswith("<"):
        raise InvalidGeoJSONArchiveError("archivo descargado parece ser HTML, no GeoJSON")
    if snippet[0] not in "{[":
        raise InvalidGeoJSONArchiveError("archivo descargado no parece iniciar como JSON")
    try:
        json.loads(snippet[: min(len(snippet), 1024)])
    except Exception:
        # Si no parsea el fragmento corto no necesariamente está mal,
        # pero al menos ya descartamos HTML y otros errores obvios.
        pass


def leer_geometrias(path: Path, source_epsg: int | None) -> tuple[list[BaseGeometry], int]:
    try:
        gdf = pyogrio.read_dataframe(path)
    except Exception as e:
        raise InvalidGeoJSONArchiveError(str(e)) from e
    if gdf.empty:
        raise RuntimeError("El GeoJSON no contiene geometrías")

    epsg = None
    if gdf.crs is not None:
        epsg = gdf.crs.to_epsg()
    if epsg is None:
        epsg = source_epsg or 4326
        gdf = gdf.set_crs(epsg, allow_override=True)

    if epsg != 4326:
        gdf = gdf.to_crs(4326)

    geoms = [geom for geom in gdf.geometry if geom is not None and not geom.is_empty]
    if not geoms:
        raise RuntimeError("El GeoJSON no contiene geometrías válidas")
    return geoms, epsg


def bounds_lonlat(geoms: list[BaseGeometry]) -> tuple[float, float, float, float]:
    min_x = min(geom.bounds[0] for geom in geoms)
    min_y = min(geom.bounds[1] for geom in geoms)
    max_x = max(geom.bounds[2] for geom in geoms)
    max_y = max(geom.bounds[3] for geom in geoms)
    if min_x == max_x:
        min_x -= 0.001
        max_x += 0.001
    if min_y == max_y:
        min_y -= 0.001
        max_y += 0.001
    return normalizar_bbox_lonlat(min_x, min_y, max_x, max_y)


def wm_to_pixel(x: float, y: float, bbox_3857: tuple[float, float, float, float], width: int, height: int) -> tuple[float, float]:
    min_x, min_y, max_x, max_y = bbox_3857
    dx = max(max_x - min_x, 1.0)
    dy = max(max_y - min_y, 1.0)
    px = (x - min_x) / dx * width
    py = (max_y - y) / dy * height
    return px, py


def reproyectar_a_3857(geom: BaseGeometry) -> BaseGeometry:
    transformer = Transformer.from_crs(4326, 3857, always_xy=True)
    return shapely_transform(transformer.transform, geom)


def geometry_to_pixels(
    geom: BaseGeometry,
    bbox_3857: tuple[float, float, float, float],
    width: int,
    height: int,
) -> BaseGeometry:
    wm_geom = reproyectar_a_3857(geom)
    return shapely_transform(
        lambda x, y, z=None: wm_to_pixel(x, y, bbox_3857, width, height),
        wm_geom,
    )


def coords_pairs(coords: Iterable) -> list[tuple[float, float]]:
    return [(float(x), float(y)) for x, y in coords]


def draw_polygon_mask(draw: ImageDraw.ImageDraw, geom: Polygon) -> None:
    exterior = coords_pairs(geom.exterior.coords)
    if len(exterior) >= 3:
        draw.polygon(exterior, fill=255)
    for interior in geom.interiors:
        ring = coords_pairs(interior.coords)
        if len(ring) >= 3:
            draw.polygon(ring, fill=0)


def render_polygon(
    overlay: Image.Image,
    geom: Polygon | MultiPolygon,
    fill_rgba: tuple[int, int, int, int],
    stroke_rgba: tuple[int, int, int, int],
    stroke_width: int,
) -> None:
    mask = Image.new("L", overlay.size, 0)
    mask_draw = ImageDraw.Draw(mask)
    poly_draw = ImageDraw.Draw(overlay, "RGBA")

    polygons = geom.geoms if isinstance(geom, MultiPolygon) else [geom]
    for polygon in polygons:
        draw_polygon_mask(mask_draw, polygon)
        exterior = coords_pairs(polygon.exterior.coords)
        if len(exterior) >= 3:
            poly_draw.line(exterior, fill=stroke_rgba, width=stroke_width, joint="curve")

    fill_layer = Image.new("RGBA", overlay.size, fill_rgba)
    transparent = Image.new("RGBA", overlay.size, (0, 0, 0, 0))
    masked_fill = Image.composite(fill_layer, transparent, mask)
    overlay.alpha_composite(masked_fill)


def render_line(draw: ImageDraw.ImageDraw, geom: LineString | MultiLineString, color: tuple[int, int, int, int], width: int) -> None:
    lines = geom.geoms if isinstance(geom, MultiLineString) else [geom]
    for line in lines:
        coords = coords_pairs(line.coords)
        if len(coords) >= 2:
            draw.line(coords, fill=color, width=width, joint="curve")


def render_point(draw: ImageDraw.ImageDraw, geom: Point | MultiPoint, color: tuple[int, int, int, int], radius: int) -> None:
    points = geom.geoms if isinstance(geom, MultiPoint) else [geom]
    for point in points:
        x, y = float(point.x), float(point.y)
        draw.ellipse((x - radius, y - radius, x + radius, y + radius), fill=color)


def render_geometry(
    overlay: Image.Image,
    geom: BaseGeometry,
    fill_rgba: tuple[int, int, int, int],
    stroke_rgba: tuple[int, int, int, int],
) -> None:
    if geom.is_empty:
        return
    draw = ImageDraw.Draw(overlay, "RGBA")
    if isinstance(geom, (Polygon, MultiPolygon)):
        render_polygon(overlay, geom, fill_rgba=fill_rgba, stroke_rgba=stroke_rgba, stroke_width=1)
    elif isinstance(geom, (LineString, MultiLineString)):
        render_line(draw, geom, color=stroke_rgba, width=3)
    elif isinstance(geom, (Point, MultiPoint)):
        render_point(draw, geom, color=stroke_rgba, radius=2)
    elif isinstance(geom, GeometryCollection):
        for part in geom.geoms:
            render_geometry(overlay, part, fill_rgba, stroke_rgba)


def componer_mapa_desde_geojson(
    geojson_url: str,
    output: Path,
    source_epsg: int | None = 4326,
    width: int = 400,
    height: int | None = None,
    padding_ratio: float = 0.15,
    color_hex: str = "#5265c7",
    opacity: float = 0.5,
) -> Path:
    rgb = ImageColor.getrgb(color_hex)
    alpha = max(0, min(255, round(opacity * 255)))
    fill_rgba = (rgb[0], rgb[1], rgb[2], alpha)
    stroke_rgba = (rgb[0], rgb[1], rgb[2], min(255, max(alpha, 180)))

    with tempfile.TemporaryDirectory(prefix="geodatos-geojson-map-") as tmpdir:
        tmp_geojson = Path(tmpdir) / "dataset.geojson"
        descargar_geojson(geojson_url, tmp_geojson)
        geoms, _ = leer_geometrias(tmp_geojson, source_epsg=source_epsg)
        min_x, min_y, max_x, max_y = bounds_lonlat(geoms)
        min_x, min_y, max_x, max_y = expandir_bbox(
            min_x, min_y, max_x, max_y, padding_ratio=padding_ratio
        )
        min_x, min_y, max_x, max_y = normalizar_bbox_lonlat(min_x, min_y, max_x, max_y)

        width, height = dimensiones_objetivo(min_x, min_y, max_x, max_y, width, height)
        base, zoom, bbox_3857 = render_basemap(min_x, min_y, max_x, max_y, width, height)

        overlay = Image.new("RGBA", base.size, (0, 0, 0, 0))
        for geom in geoms:
            px_geom = geometry_to_pixels(geom, bbox_3857, width, height)
            render_geometry(overlay, px_geom, fill_rgba=fill_rgba, stroke_rgba=stroke_rgba)

        compuesto = Image.alpha_composite(base, overlay)
        output.parent.mkdir(parents=True, exist_ok=True)
        compuesto.save(output)
        print(f"mapa desde geojson guardado en {output}")
        print(f"zoom={zoom}")
        return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--geojson-url", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--source-epsg", type=int, default=4326)
    parser.add_argument("--width", type=int, default=400)
    parser.add_argument("--height", type=int, default=None)
    parser.add_argument("--padding-ratio", type=float, default=0.15)
    parser.add_argument("--color", default="#5265c7")
    parser.add_argument("--opacity", type=float, default=0.5)
    args = parser.parse_args()

    componer_mapa_desde_geojson(
        geojson_url=args.geojson_url,
        output=Path(args.output),
        source_epsg=args.source_epsg,
        width=args.width,
        height=args.height,
        padding_ratio=args.padding_ratio,
        color_hex=args.color,
        opacity=args.opacity,
    )


if __name__ == "__main__":
    main()

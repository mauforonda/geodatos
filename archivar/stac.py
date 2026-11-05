"""Catalogo STAC estatico de los datasets archivados.

Sigue la estructura de https://github.com/cholmes/portolan-nl-catalog: un catalogo raiz, un catalogo por
fuente y una coleccion por dataset, todos con enlaces relativos entre si y sin items:

    archivar/catalog.json
    archivar/publicados/<geoserver>/catalog.json
    archivar/publicados/<geoserver>/<dataset>/collection.json

Cada corrida que perfila un dataset escribe su coleccion (`construir_collection`). Los catalogos se reconstruyen
a partir de las colecciones que existen (`escribir_catalogos`).
"""

import json
from pathlib import Path
from typing import Optional
from urllib.parse import quote

BASE_DIR = Path(__file__).resolve().parent
PUBLICADOS = BASE_DIR / "publicados"
CATALOGO_RAIZ = BASE_DIR / "catalog.json"

STAC_VERSION = "1.1.0"
ID_RAIZ = "geodatos-bolivia"
TITULO_RAIZ = "Datos geográficos de Bolivia"
DESCRIPCION_RAIZ = (
    "Capas vectoriales de geoservidores de Bolivia, archivadas en Archive.org como GeoParquet y PMTiles. "
    "Cada fuente es un catálogo y cada capa una colección."
)
EXTENSIONES = {
    "tabla": "https://stac-extensions.github.io/table/v1.2.0/schema.json",
    "vector": "https://stac-extensions.github.io/vector/v0.1.0/schema.json",
    "proyeccion": "https://stac-extensions.github.io/projection/v2.0.0/schema.json",
    "archivo": "https://stac-extensions.github.io/file/v2.1.0/schema.json",
    "mapas_web": "https://stac-extensions.github.io/web-map-links/v1.3.0/schema.json",
}
TIPO_JSON = "application/json"
PREFIJO_MULTIHASH_SHA256 = "1220"


def escribir(path: Path, datos: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(datos, f, ensure_ascii=False, indent=2)
        f.write("\n")


def url_archive(item: str, archivo: str) -> str:
    return f"https://archive.org/download/{quote(item)}/{quote(archivo)}"


def enlaces_base(raiz: str, padre: Optional[str], titulo_padre: Optional[str]) -> list[dict]:
    enlaces = [{"rel": "root", "href": raiz, "type": TIPO_JSON, "title": TITULO_RAIZ}]
    if padre is not None:
        enlaces.append({"rel": "parent", "href": padre, "type": TIPO_JSON, "title": titulo_padre})
    return enlaces


def columnas_tabla(perfil: dict) -> list[dict]:
    descripciones = {c["name"]: c.get("description") for c in perfil["campos"]}
    columnas = []
    for nombre, tipo in perfil["tipos"].items():
        columna = {"name": nombre, "type": tipo}
        if descripciones.get(nombre):
            columna["description"] = descripciones[nombre]
        columnas.append(columna)
    return columnas


def construir_collection(metadata: dict, perfil: dict, pmtiles: Optional[dict] = None) -> dict:
    """Coleccion de un dataset archivado con geoparquet normalizado y, si existe, sus teselas."""
    item = metadata["archive_item"]
    titulo = (metadata.get("titulo") or metadata["nombre"]).strip()
    descripcion = (metadata.get("descripcion") or "").strip() or titulo
    ows = (metadata.get("ows") or "").strip()
    fuente = (metadata.get("fuente") or metadata.get("geoserver") or "").strip()
    titulo_fuente = fuente or metadata["geoserver"]

    extensiones = [EXTENSIONES[k] for k in ("tabla", "vector", "proyeccion", "archivo")]
    activos = {
        "data": {
            "href": url_archive(item, "dataset.geoparquet"),
            "type": "application/vnd.apache.parquet",
            "title": "GeoParquet",
            "roles": ["data"],
            "proj:code": "EPSG:4326",
            "file:size": perfil["bytes"],
            "file:checksum": PREFIJO_MULTIHASH_SHA256 + perfil["sha256"],
        },
    }
    enlaces = enlaces_base("../../../catalog.json", "../catalog.json", titulo_fuente)
    if ows:
        enlaces.append({"rel": "via", "href": ows, "type": "text/html", "title": "Servicio de origen"})
    coleccion = {
        "type": "Collection",
        "id": item,
        "stac_version": STAC_VERSION,
        "stac_extensions": extensiones,
        "title": titulo,
        "description": descripcion,
        "license": "other",
        "extent": {
            "spatial": {"bbox": [perfil["bbox"]]},
            "temporal": {"interval": [[None, None]]},
        },
        "providers": [
            {"name": titulo_fuente, "roles": ["producer"], **({"url": ows} if ows else {})},
            {"name": "Internet Archive", "roles": ["host"], "url": f"https://archive.org/details/{quote(item)}"},
        ],
        "table:primary_geometry": perfil["geometria"],
        "table:row_count": perfil["filas"],
        "table:columns": columnas_tabla(perfil),
        "summaries": {"vector:geometry_types": perfil["tipos_geometria"]},
        "assets": activos,
    }
    activos["datapackage"] = {
        "href": "./datapackage.json",
        "type": TIPO_JSON,
        "title": "Data Package: columnas, tipos y categorías",
        "roles": ["metadata"],
    }
    if pmtiles:
        coleccion["stac_extensions"].append(EXTENSIONES["mapas_web"])
        href = url_archive(item, "dataset.pmtiles")
        activos["pmtiles"] = {
            "href": href,
            "type": "application/vnd.pmtiles",
            "title": "Teselas vectoriales (PMTiles)",
            "roles": ["visual"],
            "file:size": pmtiles["bytes"],
            "file:checksum": PREFIJO_MULTIHASH_SHA256 + pmtiles["sha256"],
        }
        enlaces.append(
            {
                "rel": "pmtiles",
                "href": href,
                "type": "application/vnd.pmtiles",
                "pmtiles:layers": [pmtiles["capa"]],
            }
        )
        coleccion["pmtiles:min_zoom"] = pmtiles["min_zoom"]
        coleccion["pmtiles:max_zoom"] = pmtiles["max_zoom"]
        coleccion["pmtiles:tile_type"] = "mvt"
        coleccion["pmtiles:center"] = pmtiles["centro"]
        coleccion["pmtiles:properties"] = pmtiles["propiedades"]
    coleccion["links"] = enlaces
    return coleccion


def leer_json(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def hijos(carpeta: Path, nombre_archivo: str) -> list[dict]:
    """Enlaces `child` a los `nombre_archivo` de las subcarpetas, con el titulo de cada uno."""
    enlaces = []
    for path in sorted(carpeta.glob(f"*/{nombre_archivo}")):
        enlaces.append(
            {
                "rel": "child",
                "href": f"./{path.parent.name}/{nombre_archivo}",
                "type": TIPO_JSON,
                "title": leer_json(path).get("title", path.parent.name),
            }
        )
    return enlaces


def catalogo_fuente(
    raiz: str, id_: str, titulo: str, descripcion: str, hijos_: list[dict], via: Optional[str]
) -> dict:
    enlaces = enlaces_base(raiz, raiz, TITULO_RAIZ)
    if via:
        enlaces.append({"rel": "via", "href": via, "type": "text/html", "title": "Servicio de origen"})
    return {
        "type": "Catalog",
        "id": id_,
        "stac_version": STAC_VERSION,
        "title": titulo,
        "description": descripcion,
        "links": enlaces + hijos_,
    }


def escribir_catalogo_fuente(carpeta: Path) -> Optional[Path]:
    """Catalogo de `publicados/<fuente>/` con todas sus colecciones. None si no tiene."""
    colecciones = hijos(carpeta, "collection.json")
    if not colecciones:
        return None
    metadata = next((leer_json(m) for m in sorted(carpeta.glob("*/metadata.json"))), {})
    titulo = (metadata.get("fuente") or metadata.get("geoserver") or carpeta.name).strip()
    ows = metadata.get("ows")
    descripcion = f"Capas archivadas del servicio {ows}." if ows else f"Capas archivadas de {titulo}."
    destino = carpeta / "catalog.json"
    escribir(destino, catalogo_fuente("../../catalog.json", carpeta.name, titulo, descripcion, colecciones, ows))
    return destino


def escribir_catalogo_raiz() -> Path:
    """Catalogo raiz con un hijo por cada catalogo de fuente existente."""
    raiz = CATALOGO_RAIZ
    catalogos = sorted(PUBLICADOS.glob("*/catalog.json"))
    hijos_raiz = [
        {
            "rel": "child",
            "href": f"./{path.parent.relative_to(raiz.parent).as_posix()}/catalog.json",
            "type": TIPO_JSON,
            "title": leer_json(path)["title"],
        }
        for path in catalogos
    ]
    escribir(
        raiz,
        {
            "type": "Catalog",
            "id": ID_RAIZ,
            "stac_version": STAC_VERSION,
            "title": TITULO_RAIZ,
            "description": DESCRIPCION_RAIZ,
            "links": [{"rel": "root", "href": "./catalog.json", "type": TIPO_JSON, "title": TITULO_RAIZ}, *hijos_raiz],
        },
    )
    return raiz


def actualizar_catalogos(carpeta_fuente: Path) -> list[Path]:
    """Catalogo de una fuente y raiz, tras escribir una coleccion nueva en esa fuente."""
    fuente = escribir_catalogo_fuente(carpeta_fuente)
    return [*([fuente] if fuente else []), escribir_catalogo_raiz()]


def escribir_catalogos() -> list[Path]:
    """Reconstruye todos los catalogos con las colecciones existentes."""
    fuentes = [escribir_catalogo_fuente(c) for c in sorted(p for p in PUBLICADOS.iterdir() if p.is_dir())]
    return [*[f for f in fuentes if f], escribir_catalogo_raiz()]


def main() -> int:
    catalogos = escribir_catalogos()
    print(f"catalogos: {len(catalogos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

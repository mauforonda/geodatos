#!/usr/bin/env python3

import argparse
import csv
import json
import re
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
ARCHIVAR_DIR = ROOT_DIR / "archivar"
DESCUBRIR_DIR = ROOT_DIR / "descubrir"
PAQUETES_CSV = ARCHIVAR_DIR / "paquetes.csv"
DIRECTORIO_JSON = DESCUBRIR_DIR / "directorio.json"
PUBLICADOS_DIR = ARCHIVAR_DIR / "publicados"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "dist" / "data" / "archivar.json"


def slug(texto: str) -> str:
    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", texto.strip())).strip("_").lower()


def leer_directorio() -> list[dict]:
    with open(DIRECTORIO_JSON, encoding="utf-8") as f:
        return json.load(f)


def leer_paquetes() -> list[dict]:
    with open(PAQUETES_CSV, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def leer_catalogos_historicos() -> list[dict]:
    rows = []
    for path in sorted(ARCHIVAR_DIR.glob("*/catalogo.csv")):
        with open(path, encoding="utf-8", newline="") as f:
            rows.extend(csv.DictReader(f))
    return rows


def ordenar_filas(rows: list[dict]) -> list[dict]:
    return sorted(
        rows,
        key=lambda row: (
            (row.get("fecha_archivado") or "").strip(),
            row.get("geoserver") or "",
            row.get("nombre") or "",
        ),
        reverse=True,
    )


def compactar_fuentes(
    directorio: list[dict], fuentes_extra: list[tuple[str, str]]
) -> tuple[list[list[str]], dict[str, int]]:
    fuentes = []
    indices = {}
    registros = {
        entrada["nombre"]: (entrada["nombre"], (entrada.get("descripcion") or "").strip())
        for entrada in directorio
    }
    for geoserver, fuente in fuentes_extra:
        registros.setdefault(geoserver, (geoserver, (fuente or geoserver).strip()))

    for geoserver, fuente in sorted(registros.values(), key=lambda item: item[0]):
        indice = len(fuentes)
        indices[geoserver] = indice
        fuentes.append(
            [
                geoserver,
                fuente,
            ]
        )
    return fuentes, indices


def ruta_sample(coleccion: str, geoserver: str, nombre: str) -> Path:
    if coleccion and coleccion != "actual":
        return ARCHIVAR_DIR / coleccion / "publicados" / slug(nombre) / "sample.json"
    return PUBLICADOS_DIR / slug(geoserver) / slug(nombre) / "sample.json"


def leer_sample(coleccion: str, geoserver: str, nombre: str) -> list[list]:
    path = ruta_sample(coleccion, geoserver, nombre)
    if not path.exists():
        return []

    with open(path, encoding="utf-8") as f:
        sample = json.load(f)

    if not isinstance(sample, dict):
        return []

    return [[str(key), value] for key, value in sample.items()]


def normalizar_bool(value: str) -> bool:
    return str(value).strip().lower() == "true"


def archive_download_url(item: str, filename: str) -> str:
    return f"https://archive.org/download/{item}/{filename}"


def compactar_paquete(paquete: dict, indice_fuente: int, sample: list[list]) -> list:
    archive_item = (paquete.get("archive_item") or "").strip()
    geojson_url = (paquete.get("geojson_url") or "").strip()
    geoparquet_url = (paquete.get("geoparquet_url") or "").strip()
    if archive_item and "geojson_url" not in paquete:
        geojson_url = archive_download_url(archive_item, "dataset.geojson")
    if archive_item and "geoparquet_url" not in paquete:
        geoparquet_url = archive_download_url(archive_item, "dataset.geoparquet")

    return [
        indice_fuente,
        paquete["nombre"],
        (paquete.get("titulo") or "").strip(),
        (paquete.get("descripcion") or "").strip(),
        (paquete.get("fecha_archivado") or "").strip(),
        archive_item,
        geojson_url,
        geoparquet_url,
        [
            1 if normalizar_bool(paquete.get("tiene_map_png", "")) else 0,
            1 if normalizar_bool(paquete.get("tiene_sample_json", "")) else 0,
        ],
        sample,
    ]


def construir_payload() -> dict:
    directorio = leer_directorio()
    paquetes_actuales = []
    for row in leer_paquetes():
        row = dict(row)
        row["coleccion"] = "actual"
        paquetes_actuales.append(row)

    paquetes_historicos = []
    for row in leer_catalogos_historicos():
        row = dict(row)
        row["coleccion"] = (row.get("coleccion") or "").strip() or "historico"
        paquetes_historicos.append(row)

    paquetes = ordenar_filas(paquetes_actuales + paquetes_historicos)
    fuentes_extra = sorted(
        {
            (
                row["geoserver"],
                (row.get("fuente") or row["geoserver"]).strip(),
            )
            for row in paquetes_historicos
        }
    )
    fuentes, indices_fuente = compactar_fuentes(directorio, fuentes_extra)

    filas = []
    for paquete in paquetes:
        geoserver = paquete["geoserver"]
        if geoserver not in indices_fuente:
            continue
        sample = leer_sample(paquete.get("coleccion", "actual"), geoserver, paquete["nombre"])
        filas.append(compactar_paquete(paquete, indices_fuente[geoserver], sample))

    return {
        "v": 1,
        "k": {
            "s": ["geoserver", "fuente"],
            "r": [
                "source",
                "nombre",
                "titulo",
                "descripcion",
                "fecha_archivado",
                "archive_item",
                "geojson_url",
                "geoparquet_url",
                "flags",
                "sample",
            ],
            "f": ["map", "sample"],
            "a": ["key", "value"],
        },
        "s": fuentes,
        "r": filas,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un JSON compacto para la vista beta del indice archivado."
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=DEFAULT_OUTPUT,
        help=f"Ruta de salida del JSON generado. Default: {DEFAULT_OUTPUT}",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    payload = construir_payload()
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with open(args.output, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, separators=(",", ":"))
        f.write("\n")

    size = args.output.stat().st_size
    print(
        f"archivar: {len(payload['r'])} paquetes, {len(payload['s'])} fuentes, {size / (1024 * 1024):.2f} MiB -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

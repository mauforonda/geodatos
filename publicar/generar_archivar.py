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


def compactar_fuentes(directorio: list[dict]) -> tuple[list[list[str]], dict[str, int]]:
    fuentes = []
    indices = {}
    for entrada in sorted(directorio, key=lambda item: item["nombre"]):
        indice = len(fuentes)
        indices[entrada["nombre"]] = indice
        fuentes.append(
            [
                entrada["nombre"],
                (entrada.get("descripcion") or "").strip(),
            ]
        )
    return fuentes, indices


def ruta_sample(geoserver: str, nombre: str) -> Path:
    return PUBLICADOS_DIR / slug(geoserver) / slug(nombre) / "sample.json"


def leer_sample(geoserver: str, nombre: str) -> list[list]:
    path = ruta_sample(geoserver, nombre)
    if not path.exists():
        return []

    with open(path, encoding="utf-8") as f:
        sample = json.load(f)

    if not isinstance(sample, dict):
        return []

    return [[str(key), value] for key, value in sample.items()]


def compactar_paquete(paquete: dict, indice_fuente: int, sample: list[list]) -> list:
    return [
        indice_fuente,
        paquete["nombre"],
        (paquete.get("titulo") or "").strip(),
        (paquete.get("descripcion") or "").strip(),
        (paquete.get("fecha_archivado") or "").strip(),
        paquete["archive_item"],
        [
            1 if paquete.get("tiene_map_png") == "True" else 0,
            1 if paquete.get("tiene_sample_json") == "True" else 0,
        ],
        sample,
    ]


def construir_payload() -> dict:
    directorio = leer_directorio()
    paquetes = leer_paquetes()
    fuentes, indices_fuente = compactar_fuentes(directorio)

    filas = []
    for paquete in paquetes:
        geoserver = paquete["geoserver"]
        if geoserver not in indices_fuente:
            continue
        sample = leer_sample(geoserver, paquete["nombre"])
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

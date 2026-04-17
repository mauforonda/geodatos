#!/usr/bin/env python3

import argparse
import csv
import json
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parent.parent
DESCUBRIR_DIR = ROOT_DIR / "descubrir"
CAPAS_CSV = DESCUBRIR_DIR / "capas.csv"
DIRECTORIO_JSON = DESCUBRIR_DIR / "directorio.json"
DEFAULT_OUTPUT = Path(__file__).resolve().parent / "dist" / "data" / "descubrir.json"


def leer_directorio() -> list[dict]:
    with open(DIRECTORIO_JSON, encoding="utf-8") as f:
        return json.load(f)


def leer_capas() -> list[dict]:
    with open(CAPAS_CSV, encoding="utf-8", newline="") as f:
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
                entrada["ows"],
            ]
        )

    return fuentes, indices


def compactar_capa(capa: dict, indice_fuente: int, fuente: dict) -> list:
    wms = bool(fuente.get("wms"))
    wfs = bool(fuente.get("wfs"))
    return [
        indice_fuente,
        capa["nombre"],
        (capa.get("titulo") or "").strip(),
        (capa.get("descripcion") or "").strip(),
        (capa.get("fecha_encontrado") or "").strip(),
        [
            1 if wms else 0,
            1 if wfs else 0,
        ],
    ]


def construir_payload() -> dict:
    directorio = leer_directorio()
    capas = leer_capas()
    fuentes, indices_fuente = compactar_fuentes(directorio)
    directorio_por_nombre = {entrada["nombre"]: entrada for entrada in directorio}

    filas = []
    activas = [capa for capa in capas if not (capa.get("fecha_removido") or "").strip()]
    for capa in reversed(activas):
        geoserver = capa["geoserver"]
        if geoserver not in indices_fuente:
            continue
        fuente = directorio_por_nombre.get(geoserver, {})
        filas.append(compactar_capa(capa, indices_fuente[geoserver], fuente))

    return {
        "v": 1,
        "k": {
            "s": ["geoserver", "fuente", "ows"],
            "r": ["source", "nombre", "titulo", "descripcion", "fecha_encontrado", "flags"],
            "f": ["wms", "wfs"],
        },
        "s": fuentes,
        "r": filas,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera un JSON compacto para la vista beta del indice descubierto."
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
        f"descubrir: {len(payload['r'])} capas activas, {len(payload['s'])} fuentes, {size / (1024 * 1024):.2f} MiB -> {args.output}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

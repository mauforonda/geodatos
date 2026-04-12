#!/usr/bin/env python3

import argparse
import csv
import sys
from pathlib import Path

import pandas as pd

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from archivar.actualizar import (  # noqa: E402
    guardar_datasets,
    guardar_paquetes,
    iniciar_sesion,
    leer_datasets,
    leer_paquetes,
    sincronizar_item_archive_org,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-items", type=int, default=None)
    parser.add_argument("--incluir-archivados", action="store_true")
    parser.add_argument("--items-file", default=None)
    return parser.parse_args()


def cargar_items(path_str: str) -> list[tuple[str, str]]:
    path = Path(path_str)
    if not path.exists():
        raise FileNotFoundError(f"No existe {path}")

    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path, keep_default_na=False)
        if {"geoserver", "nombre"}.issubset(df.columns):
            items = [(str(row.geoserver).strip(), str(row.nombre).strip()) for row in df.itertuples(index=False)]
        else:
            with open(path, newline="") as f:
                reader = csv.reader(f)
                items = []
                for row in reader:
                    if len(row) < 2:
                        continue
                    items.append((str(row[0]).strip(), str(row[1]).strip()))
    else:
        items = []
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "," in line:
                    geoserver, nombre = line.split(",", 1)
                elif "\t" in line:
                    geoserver, nombre = line.split("\t", 1)
                else:
                    raise ValueError(
                        f"Linea invalida en {path}: {line}. Use 'geoserver,nombre' o 'geoserver<TAB>nombre'"
                    )
                items.append((geoserver.strip(), nombre.strip()))

    vistos = set()
    ordenados = []
    for item in items:
        if not item[0] or not item[1] or item in vistos:
            continue
        vistos.add(item)
        ordenados.append(item)
    return ordenados


def main() -> int:
    args = parse_args()
    sesion = iniciar_sesion()
    datasets = leer_datasets()
    paquetes = leer_paquetes()

    if args.items_file:
        items = cargar_items(args.items_file)
        items_df = pd.DataFrame(items, columns=["geoserver", "nombre"])
        candidatos = datasets.merge(items_df, on=["geoserver", "nombre"], how="inner")
        if not args.incluir_archivados:
            candidatos = candidatos[~candidatos["archivado"]].copy()
        candidatos = candidatos.drop_duplicates(subset=["geoserver", "nombre"], keep="first")
    else:
        if args.incluir_archivados:
            candidatos = datasets.copy()
        else:
            candidatos = datasets[~datasets["archivado"]].copy()

    sincronizados = 0
    revisados = 0

    for fila in candidatos.itertuples(index=False):
        if args.max_items is not None and revisados >= args.max_items:
            break
        revisados += 1

        ok, datasets, paquetes = sincronizar_item_archive_org(
            sesion=sesion,
            datasets=datasets,
            paquetes=paquetes,
            geoserver=fila.geoserver,
            nombre=fila.nombre,
        )
        if not ok:
            continue

        guardar_paquetes(paquetes)
        guardar_datasets(datasets)
        sincronizados += 1
        print(f"sincronizado {fila.geoserver} / {fila.nombre}")

    print(f"items revisados: {revisados}")
    print(f"items sincronizados: {sincronizados}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

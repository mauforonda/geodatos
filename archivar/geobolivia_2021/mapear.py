#!/usr/bin/env python3

import argparse
import csv
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Optional
from urllib.parse import quote

import requests

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from archivar.componer_mapa_geojson import componer_mapa_desde_geojson
from archivar.componer_mapa_geojson import InvalidGeoJSONArchiveError


BASE_DIR = Path(__file__).resolve().parent
CATALOGO_CSV = BASE_DIR / "catalogo.csv"
PUBLICADOS_DIR = BASE_DIR / "publicados"
DEFAULT_MAX_BYTES = 300 * 1024 * 1024
USER_AGENT = "geodatos-geobolivia-cover/1.0"


def slug(texto: str) -> str:
    import re

    return re.sub(r"_+", "_", re.sub(r"[^0-9A-Za-z]+", "_", texto.strip())).strip("_").lower()


def leer_catalogo() -> list[dict]:
    with open(CATALOGO_CSV, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def escribir_catalogo(rows: list[dict]) -> None:
    if not rows:
        return
    with open(CATALOGO_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def ordenar_rows(rows: list[dict]) -> list[dict]:
    def key(row: dict):
        size = row.get("size_bytes", "").strip()
        size_int = int(size) if size else 10**18
        return (size_int, row.get("nombre", ""))

    return sorted(rows, key=key)


def dataset_dir(nombre: str) -> Path:
    return PUBLICADOS_DIR / slug(nombre)


def metadata_path(nombre: str) -> Path:
    return dataset_dir(nombre) / "metadata.json"


def map_path(nombre: str) -> Path:
    return dataset_dir(nombre) / "map.png"


def leer_metadata(nombre: str) -> dict:
    path = metadata_path(nombre)
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def escribir_metadata(nombre: str, payload: dict) -> None:
    path = metadata_path(nombre)
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
        f.write("\n")


def archive_metadata_url(item: str) -> str:
    return f"https://archive.org/metadata/{quote(item)}"


def archive_download_url(item: str, filename: str) -> str:
    return f"https://archive.org/download/{quote(item)}/{quote(filename)}"


def remote_cover_exists(item: str) -> bool:
    response = requests.get(
        archive_metadata_url(item),
        timeout=(15, 40),
        headers={"User-Agent": USER_AGENT},
    )
    response.raise_for_status()
    payload = response.json()
    filenames = {entry.get("name") for entry in payload.get("files", [])}
    return "cover.png" in filenames


def descargar_cover_remoto(item: str, destino: Path) -> bool:
    url = archive_download_url(item, "cover.png")
    response = requests.get(
        url,
        timeout=(15, 60),
        headers={"User-Agent": USER_AGENT},
    )
    if response.status_code != 200:
        return False
    destino.parent.mkdir(parents=True, exist_ok=True)
    destino.write_bytes(response.content)
    return True


def subir_cover(item: str, local_map: Path) -> None:
    try:
        from internetarchive import upload
    except ImportError as e:
        raise RuntimeError(
            "Falta la dependencia internetarchive. Instala requirements.txt antes de ejecutar este script."
        ) from e

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Faltan IA_ACCESS_KEY y/o IA_SECRET_KEY en el entorno")

    with tempfile.TemporaryDirectory(prefix="geodatos-geobolivia-cover-") as tmpdir:
        staged_cover = Path(tmpdir) / "cover.png"
        shutil.copy2(local_map, staged_cover)
        resultado = upload(
            item,
            [str(staged_cover)],
            access_key=access_key,
            secret_key=secret_key,
            verify=False,
            verbose=True,
        )
        ok = all(r.status_code in [200, 302] for r in resultado) if hasattr(resultado, "__iter__") else True
        if not ok:
            raise RuntimeError(f"La subida de cover.png a Archive.org no completó correctamente para {item}")


def actualizar_estado_local(row: dict) -> None:
    row["tiene_map_png"] = "True"
    metadata = leer_metadata(row["nombre"])
    metadata["tiene_map_png"] = True
    escribir_metadata(row["nombre"], metadata)


def dataset_listo(row: dict) -> bool:
    return (
        row.get("tiene_map_png", "").strip() == "True"
        and map_path(row["nombre"]).exists()
        and remote_cover_exists(row["archive_item"])
    )


def procesar_row(row: dict) -> str:
    nombre = row["nombre"]
    item = row["archive_item"]
    local_map = map_path(nombre)

    remote_has_cover = remote_cover_exists(item)
    local_has_map = local_map.exists()
    flag_has_map = row.get("tiene_map_png", "").strip() == "True"

    if local_has_map and remote_has_cover and flag_has_map:
        return "skip"

    if remote_has_cover and not local_has_map:
        if descargar_cover_remoto(item, local_map):
            local_has_map = True

    if not local_has_map:
        metadata = leer_metadata(nombre)
        srs = (metadata.get("srs") or "").strip()
        source_epsg: Optional[int] = 4326
        if srs.upper().startswith("EPSG:"):
            try:
                source_epsg = int(srs.split(":", 1)[1])
            except Exception:
                source_epsg = 4326

        print(f"generando mapa {nombre}")
        componer_mapa_desde_geojson(
            geojson_url=row["geojson_url"],
            output=local_map,
            source_epsg=source_epsg,
            width=400,
            color_hex="#5265c7",
            opacity=0.5,
        )

    if not remote_has_cover:
        print(f"subiendo cover.png a {item}")
        subir_cover(item, local_map)
        remote_has_cover = True

    if remote_has_cover and local_map.exists():
        actualizar_estado_local(row)
        return "ok"

    raise RuntimeError(f"No se pudo completar el estado de mapa para {nombre}")


def excluir_row(rows: list[dict], row: dict) -> None:
    nombre = row["nombre"]
    rows[:] = [current for current in rows if current["nombre"] != nombre]
    print(f"excluido del catalogo: {nombre}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Genera y sube cover.png para datasets historicos de GeoBolivia 2021."
    )
    parser.add_argument("--max-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--max-datasets", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = ordenar_rows(leer_catalogo())

    procesados = 0
    actualizados = 0
    errores = 0
    saltados_tamano = 0

    for row in list(rows):
        size_bytes = int(row["size_bytes"]) if row.get("size_bytes", "").strip() else None
        if size_bytes is not None and size_bytes > args.max_bytes:
            saltados_tamano += 1
            continue

        if args.max_datasets is not None and procesados >= args.max_datasets:
            break

        nombre = row["nombre"]
        print(f"\nprocesando {nombre}")
        try:
            status = procesar_row(row)
            if status == "ok":
                actualizados += 1
                escribir_catalogo(rows)
            procesados += 1
            print(f"estado: {status}")
        except InvalidGeoJSONArchiveError as e:
            errores += 1
            procesados += 1
            excluir_row(rows, row)
            escribir_catalogo(rows)
            print(f"error invalido en {nombre}: {e}")
        except Exception as e:
            errores += 1
            procesados += 1
            print(f"error en {nombre}: {e}")

    escribir_catalogo(rows)
    print(
        f"\nprocesados={procesados} actualizados={actualizados} errores={errores} saltados_tamano={saltados_tamano}"
    )
    return 0 if errores == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

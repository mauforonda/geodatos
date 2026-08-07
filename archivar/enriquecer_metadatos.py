#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import urllib3

from archivar.actualizar import (
    leer_directorio,
    metadata_item_archive_org,
    parse_bool,
    rutas_publicacion,
)
from descubrir.metadatos import leer_metadatos


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent
CAPAS = ROOT_DIR / "descubrir" / "capas.csv"
DATASETS = ROOT_DIR / "evaluar" / "datasets.csv"
PAQUETES = BASE_DIR / "paquetes.csv"
DIRECTORIO = ROOT_DIR / "descubrir" / "directorio.json"


def iniciar_sesion() -> urllib3.PoolManager:
    urllib3.disable_warnings()
    return urllib3.PoolManager(timeout=60, retries=2, cert_reqs="CERT_NONE")


def leer_datos() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    capas = pd.read_csv(CAPAS, keep_default_na=False)
    datasets = pd.read_csv(DATASETS, keep_default_na=False)
    paquetes = pd.read_csv(PAQUETES, keep_default_na=False)
    if "archivado" in datasets:
        datasets["archivado"] = parse_bool(datasets["archivado"])
    for columna in ["tiene_legend_png", "tiene_map_png", "tiene_sample_json"]:
        if columna in paquetes:
            paquetes[columna] = parse_bool(paquetes[columna])
    return capas, datasets, paquetes


def campo_cambia(nuevo, actual) -> bool:
    nuevo = str(nuevo or "").strip()
    actual = str(actual or "").strip()
    return bool(nuevo) and nuevo != actual


def unir_metadatos(base: pd.DataFrame, metadatos: pd.DataFrame) -> pd.DataFrame:
    return base.merge(
        metadatos,
        on=["geoserver", "nombre"],
        how="inner",
        validate="one_to_one",
        suffixes=("_local", "_remoto"),
    )


def seleccionar_reparaciones(
    metadatos: pd.DataFrame,
    capas: pd.DataFrame,
    datasets: pd.DataFrame,
    paquetes: pd.DataFrame,
) -> set[tuple[str, str]]:
    cambios = set()
    for base, campos in [
        (capas, (("titulo", "titulo"), ("descripcion", "descripcion"))),
        (paquetes, (("titulo", "titulo"), ("descripcion", "descripcion"))),
        (datasets, (("descripcion", "descripcion"),)),
    ]:
        unido = unir_metadatos(base, metadatos)
        mascara = unido.apply(
            lambda fila: any(
                campo_cambia(
                    fila[f"{remoto}_remoto"], fila[f"{local}_local"]
                )
                for remoto, local in campos
            ),
            axis=1,
        )
        cambios.update(zip(unido.loc[mascara, "geoserver"], unido.loc[mascara, "nombre"]))
    return cambios


def actualizar_capas(
    capas: pd.DataFrame, mapa: pd.DataFrame, cambios: set[tuple[str, str]]
) -> None:
    for indice in capas.index:
        clave = (capas.at[indice, "geoserver"], capas.at[indice, "nombre"])
        if clave not in cambios or clave not in mapa.index:
            continue
        entrada = mapa.loc[clave]
        if entrada["titulo"]:
            capas.at[indice, "titulo"] = entrada["titulo"]
        if entrada["descripcion"]:
            capas.at[indice, "descripcion"] = entrada["descripcion"]


def actualizar_datasets(
    datasets: pd.DataFrame, mapa: pd.DataFrame, cambios: set[tuple[str, str]]
) -> None:
    for indice in datasets.index:
        clave = (datasets.at[indice, "geoserver"], datasets.at[indice, "nombre"])
        if clave in cambios and clave in mapa.index and mapa.loc[clave, "descripcion"]:
            datasets.at[indice, "descripcion"] = mapa.loc[clave, "descripcion"]


def actualizar_metadata_publicada(
    paquetes: pd.DataFrame,
    mapa: pd.DataFrame,
    cambios: set[tuple[str, str]],
) -> list[tuple[str, str]]:
    faltantes = []
    for indice in paquetes.index:
        clave = (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        if clave not in cambios or clave not in mapa.index:
            continue
        ruta = rutas_publicacion(*clave)
        if not ruta["metadata"].exists():
            faltantes.append(clave)
            continue
        with open(ruta["metadata"], encoding="utf-8") as archivo:
            metadata = json.load(archivo)
        entrada = mapa.loc[clave]
        if entrada["titulo"]:
            metadata["titulo"] = entrada["titulo"]
        if entrada["descripcion"]:
            metadata["descripcion"] = entrada["descripcion"]
        with open(ruta["metadata"], "w", encoding="utf-8") as archivo:
            json.dump(metadata, archivo, ensure_ascii=False, indent=2)
            archivo.write("\n")
    return faltantes


def actualizar_archive_org(
    paquetes: pd.DataFrame,
    capas: pd.DataFrame,
    datasets: pd.DataFrame,
    mapa: pd.DataFrame,
    directorio: dict,
    cambios_archive: set[tuple[str, str]],
) -> list[tuple[str, str, str]]:
    from internetarchive import get_item

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Faltan IA_ACCESS_KEY y/o IA_SECRET_KEY en el entorno")

    capas_index = capas.set_index(["geoserver", "nombre"])
    errores = []
    for indice in paquetes.index:
        clave = (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        if clave not in cambios_archive:
            continue
        archive_item = str(paquetes.at[indice, "archive_item"]).strip()
        try:
            entrada = mapa.loc[clave]
            capa = capas_index.loc[clave].copy()
            titulo = str(entrada["titulo"] or paquetes.at[indice, "titulo"] or capa.get("titulo") or clave[1]).strip()
            descripcion = str(entrada["descripcion"] or paquetes.at[indice, "descripcion"] or capa.get("descripcion") or "").strip()
            capa["titulo"] = titulo
            capa["descripcion"] = descripcion
            fuente = directorio[clave[0]].get("descripcion") or clave[0]
            archive_metadata = metadata_item_archive_org(
                fuente_legible=fuente,
                geoserver=clave[0],
                nombre=clave[1],
                titulo=titulo,
                descripcion=descripcion,
                fecha_archivado=str(paquetes.at[indice, "fecha_archivado"]),
                ows=directorio[clave[0]]["ows"],
                epsg=None if pd.isna(capa.epsg) else int(capa.epsg),
                capa=capa,
                n_features=int(float(paquetes.at[indice, "n_features"])),
                tiene_legend=bool(paquetes.at[indice, "tiene_legend_png"]),
            )
            item = get_item(archive_item)
            respuesta = item.modify_metadata(
                {"title": archive_metadata["title"], "description": archive_metadata["description"]},
                access_key=access_key,
                secret_key=secret_key,
                reduced_priority=True,
            )
            if getattr(respuesta, "status_code", 200) not in [200, 201, 202]:
                raise RuntimeError(f"Metadata API devolvio {respuesta.status_code}")
            respuestas = item.upload(
                str(rutas_publicacion(*clave)["metadata"]),
                access_key=access_key,
                secret_key=secret_key,
                queue_derive=False,
                retries=2,
                retries_sleep=5,
                verify=False,
            )
            estados = [getattr(respuesta, "status_code", 200) for respuesta in respuestas]
            if any(estado not in [200, 201, 202] for estado in estados):
                raise RuntimeError(f"Subida de metadata.json devolvio {estados}")
            if entrada["titulo"]:
                paquetes.at[indice, "titulo"] = entrada["titulo"]
            if entrada["descripcion"]:
                paquetes.at[indice, "descripcion"] = entrada["descripcion"]
            guardar_local(capas, datasets, paquetes)
            print(f"actualizado Archive.org: {archive_item}")
        except Exception as error:
            errores.append((clave[0], clave[1], str(error)))
            print(f"error Archive.org {archive_item}: {error}")
    return errores


def guardar_local(capas: pd.DataFrame, datasets: pd.DataFrame, paquetes: pd.DataFrame) -> None:
    capas.to_csv(CAPAS, index=False, float_format="%.6f")
    datasets.to_csv(DATASETS, index=False, float_format="%.6f")
    paquetes.to_csv(PAQUETES, index=False, float_format="%.6f")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-archive", action="store_true")
    parser.add_argument("--max-items", type=int, default=None)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    metadatos, errores_fuentes = leer_metadatos(iniciar_sesion())
    capas, datasets, paquetes = leer_datos()
    cambios = seleccionar_reparaciones(metadatos, capas, datasets, paquetes)
    mapa = metadatos.set_index(["geoserver", "nombre"])
    cambios_archive = {
        (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        for indice in paquetes.index
        if (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"]) in cambios
    }
    if args.max_items is not None:
        cambios_archive = set(
            sorted(cambios_archive)[: max(0, args.max_items)]
        )

    print(f"metadatos disponibles: {len(metadatos)}")
    print(f"registros con cambios: {len(cambios)}")
    print(f"archivados a reparar en esta ejecución: {len(cambios_archive)}")
    if errores_fuentes:
        print(f"fuentes con errores: {len(errores_fuentes)}")
    if args.dry_run:
        return 0

    actualizar_capas(capas, mapa, cambios)
    actualizar_datasets(datasets, mapa, cambios)
    faltantes = actualizar_metadata_publicada(paquetes, mapa, cambios)
    if faltantes:
        print(f"aviso: faltan metadata.json locales para {len(faltantes)} datasets")
    guardar_local(capas, datasets, paquetes)

    if not args.skip_archive:
        errores = actualizar_archive_org(
            paquetes, capas, datasets, mapa, leer_directorio(), cambios_archive
        )
        if errores:
            print(f"errores Archive.org: {len(errores)}")
            return 1
        if errores_fuentes:
            return 1
        if not cambios_archive:
            return 2
    else:
        for indice in paquetes.index:
            clave = (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
            if clave not in cambios or clave not in mapa.index:
                continue
            if mapa.loc[clave, "titulo"]:
                paquetes.at[indice, "titulo"] = mapa.loc[clave, "titulo"]
            if mapa.loc[clave, "descripcion"]:
                paquetes.at[indice, "descripcion"] = mapa.loc[clave, "descripcion"]
        guardar_local(capas, datasets, paquetes)
        if errores_fuentes:
            return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

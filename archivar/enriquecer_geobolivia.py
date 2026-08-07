#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

import pandas as pd
import urllib3


BASE_DIR = Path(__file__).resolve().parent
ROOT_DIR = BASE_DIR.parent

from archivar.actualizar import (  # noqa: E402
    leer_directorio,
    metadata_item_archive_org,
    parse_bool,
    rutas_publicacion,
)
from descubrir.catalogo_geobolivia import (  # noqa: E402
    GEOBOLIVIA_GEOSERVER,
    leer_catalogo_geobolivia,
)


CAPAS = ROOT_DIR / "descubrir" / "capas.csv"
DATASETS = ROOT_DIR / "evaluar" / "datasets.csv"
PAQUETES = BASE_DIR / "paquetes.csv"
DIRECTORIO = ROOT_DIR / "descubrir" / "directorio.json"


def iniciar_sesion() -> urllib3.PoolManager:
    urllib3.disable_warnings()
    return urllib3.PoolManager(
        timeout=60,
        retries=2,
        cert_reqs="CERT_NONE",
    )


def leer_datos() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    capas = pd.read_csv(CAPAS, keep_default_na=False)
    datasets = pd.read_csv(DATASETS, keep_default_na=False)
    paquetes = pd.read_csv(PAQUETES, keep_default_na=False)
    for columna in ["archivado"]:
        if columna in datasets:
            datasets[columna] = parse_bool(datasets[columna])
    for columna in ["tiene_legend_png", "tiene_map_png", "tiene_sample_json"]:
        if columna in paquetes:
            paquetes[columna] = parse_bool(paquetes[columna])
    return capas, datasets, paquetes


def unir_catalogo_capas(
    catalogo: pd.DataFrame,
    capas: pd.DataFrame,
) -> pd.DataFrame:
    capas_fuente = capas[capas["geoserver"].eq(GEOBOLIVIA_GEOSERVER)].copy()
    return catalogo.merge(
        capas_fuente,
        left_on="technical_name",
        right_on="nombre",
        how="inner",
        validate="one_to_one",
    )


def claves(filas: pd.DataFrame) -> set[tuple[str, str]]:
    return set(zip(filas["geoserver"], filas["nombre"]))


def campo_cambia(nuevo, actual) -> bool:
    nuevo = str(nuevo or "").strip()
    actual = str(actual or "").strip()
    return bool(nuevo) and nuevo != actual


def seleccionar_reparaciones(
    catalogo: pd.DataFrame,
    capas: pd.DataFrame,
    datasets: pd.DataFrame,
    paquetes: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    unido = unir_catalogo_capas(catalogo, capas)
    unido["cambio_capa"] = unido.apply(
        lambda fila: campo_cambia(fila["display_title"], fila["titulo"])
        or campo_cambia(fila["abstract"], fila["descripcion"]),
        axis=1,
    )

    paquetes_fuente = paquetes[
        paquetes["geoserver"].eq(GEOBOLIVIA_GEOSERVER)
    ].copy()
    paquetes_catalogo = paquetes_fuente.merge(
        catalogo,
        left_on="nombre",
        right_on="technical_name",
        how="inner",
        validate="one_to_one",
    )
    paquetes_catalogo["cambio_paquete"] = paquetes_catalogo.apply(
        lambda fila: campo_cambia(fila["display_title"], fila["titulo"])
        or campo_cambia(fila["abstract"], fila["descripcion"]),
        axis=1,
    )

    datasets_fuente = datasets[datasets["geoserver"].eq(GEOBOLIVIA_GEOSERVER)].copy()
    datasets_catalogo = datasets_fuente.merge(
        catalogo,
        left_on="nombre",
        right_on="technical_name",
        how="inner",
        validate="one_to_one",
    )
    datasets_catalogo["cambio_dataset"] = datasets_catalogo.apply(
        lambda fila: campo_cambia(fila["abstract"], fila["descripcion"]),
        axis=1,
    )

    cambios = claves(unido[unido["cambio_capa"]]) | claves(
        paquetes_catalogo[paquetes_catalogo["cambio_paquete"]]
    ) | claves(datasets_catalogo[datasets_catalogo["cambio_dataset"]])
    seleccion = catalogo[
        catalogo["technical_name"].isin({nombre for _, nombre in cambios})
    ].copy()
    seleccion["geoserver"] = GEOBOLIVIA_GEOSERVER
    return unido, seleccion


def actualizar_capas(capas: pd.DataFrame, catalogo: pd.DataFrame, cambios: set) -> int:
    actualizadas = 0
    for indice in capas.index:
        clave = (capas.at[indice, "geoserver"], capas.at[indice, "nombre"])
        if clave not in cambios:
            continue
        fila = catalogo[catalogo["technical_name"].eq(clave[1])]
        if fila.empty:
            continue
        entrada = fila.iloc[0]
        if entrada["display_title"]:
            capas.at[indice, "titulo"] = entrada["display_title"]
        if entrada["abstract"]:
            capas.at[indice, "descripcion"] = entrada["abstract"]
        actualizadas += 1
    return actualizadas


def actualizar_datasets(datasets: pd.DataFrame, catalogo: pd.DataFrame, cambios: set) -> int:
    actualizadas = 0
    for indice in datasets.index:
        clave = (datasets.at[indice, "geoserver"], datasets.at[indice, "nombre"])
        if clave not in cambios:
            continue
        fila = catalogo[catalogo["technical_name"].eq(clave[1])]
        if fila.empty:
            continue
        if fila.iloc[0]["abstract"]:
            datasets.at[indice, "descripcion"] = fila.iloc[0]["abstract"]
        actualizadas += 1
    return actualizadas


def actualizar_paquetes(
    paquetes: pd.DataFrame,
    catalogo: pd.DataFrame,
    cambios: set,
) -> int:
    actualizadas = 0
    for indice in paquetes.index:
        clave = (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        if clave not in cambios:
            continue
        fila = catalogo[catalogo["technical_name"].eq(clave[1])]
        if fila.empty:
            continue
        entrada = fila.iloc[0]
        if entrada["display_title"]:
            paquetes.at[indice, "titulo"] = entrada["display_title"]
        if entrada["abstract"]:
            paquetes.at[indice, "descripcion"] = entrada["abstract"]
        actualizadas += 1
    return actualizadas


def actualizar_metadata_publicada(
    paquetes: pd.DataFrame,
    catalogo: pd.DataFrame,
    cambios: set,
) -> list[tuple[str, str]]:
    faltantes = []
    for indice in paquetes.index:
        clave = (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        if clave not in cambios:
            continue
        ruta = rutas_publicacion(*clave)
        if not ruta["metadata"].exists():
            faltantes.append(clave)
            continue
        with open(ruta["metadata"], encoding="utf-8") as archivo:
            metadata = json.load(archivo)
        entrada = catalogo[catalogo["technical_name"].eq(clave[1])].iloc[0]
        if entrada["display_title"]:
            metadata["titulo"] = entrada["display_title"]
        if entrada["abstract"]:
            metadata["descripcion"] = entrada["abstract"]
        with open(ruta["metadata"], "w", encoding="utf-8") as archivo:
            json.dump(metadata, archivo, ensure_ascii=False, indent=2)
            archivo.write("\n")
    return faltantes


def actualizar_archive_org(
    paquetes: pd.DataFrame,
    capas: pd.DataFrame,
    datasets: pd.DataFrame,
    catalogo: pd.DataFrame,
    directorio: dict,
    cambios: set,
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
        if clave not in cambios:
            continue
        archive_item = str(paquetes.at[indice, "archive_item"]).strip()
        try:
            catalogo_fila = catalogo[catalogo["technical_name"].eq(clave[1])].iloc[0]
            capa = capas_index.loc[clave].copy()
            titulo = str(catalogo_fila["display_title"] or "").strip() or str(
                paquetes.at[indice, "titulo"] or capa.get("titulo") or clave[1]
            ).strip()
            descripcion = str(catalogo_fila["abstract"] or "").strip() or str(
                paquetes.at[indice, "descripcion"] or capa.get("descripcion") or ""
            ).strip()
            capa["titulo"] = titulo
            capa["descripcion"] = descripcion
            fuente = directorio[GEOBOLIVIA_GEOSERVER].get("descripcion") or GEOBOLIVIA_GEOSERVER
            archive_metadata = metadata_item_archive_org(
                fuente_legible=fuente,
                geoserver=clave[0],
                nombre=clave[1],
                titulo=titulo,
                descripcion=descripcion,
                fecha_archivado=str(paquetes.at[indice, "fecha_archivado"]),
                ows=directorio[GEOBOLIVIA_GEOSERVER]["ows"],
                epsg=None if pd.isna(capa.epsg) else int(capa.epsg),
                capa=capa,
                n_features=int(float(paquetes.at[indice, "n_features"])),
                tiene_legend=bool(paquetes.at[indice, "tiene_legend_png"]),
            )
            item = get_item(archive_item)
            respuesta = item.modify_metadata(
                {
                    "title": archive_metadata["title"],
                    "description": archive_metadata["description"],
                },
                access_key=access_key,
                secret_key=secret_key,
                reduced_priority=True,
            )
            if getattr(respuesta, "status_code", 200) not in [200, 201, 202]:
                raise RuntimeError(f"Metadata API devolvio {respuesta.status_code}")

            metadata_path = rutas_publicacion(*clave)["metadata"]
            respuestas = item.upload(
                str(metadata_path),
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

            if catalogo_fila["display_title"]:
                paquetes.at[indice, "titulo"] = catalogo_fila["display_title"]
            if catalogo_fila["abstract"]:
                paquetes.at[indice, "descripcion"] = catalogo_fila["abstract"]
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
    parser.add_argument(
        "--max-items",
        type=int,
        default=None,
        help="Limita los items remotos procesados en esta ejecución.",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    sesion = iniciar_sesion()
    catalogo, fecha_catalogo = leer_catalogo_geobolivia(sesion)
    capas, datasets, paquetes = leer_datos()
    unido, seleccion = seleccionar_reparaciones(catalogo, capas, datasets, paquetes)
    cambios = set(zip(seleccion["geoserver"], seleccion["technical_name"]))
    cambios_archive = [
        (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"])
        for indice in paquetes.index
        if (paquetes.at[indice, "geoserver"], paquetes.at[indice, "nombre"]) in cambios
    ]
    if args.max_items is not None:
        cambios_archive = cambios_archive[: max(0, args.max_items)]
    cambios_archive = set(cambios_archive)

    print(f"catalogo: {len(catalogo)} datasets ({fecha_catalogo or 'sin fecha'})")
    print(f"capas coincidentes: {len(unido)}")
    print(f"capas sin correspondencia: {len(catalogo) - len(unido)}")
    print(f"datasets a reparar: {len(cambios)}")
    print(f"archivados a reparar en esta ejecución: {len(cambios_archive)}")
    if args.dry_run:
        return 0

    actualizar_capas(capas, catalogo, cambios)
    actualizar_datasets(datasets, catalogo, cambios)
    faltantes = actualizar_metadata_publicada(paquetes, catalogo, cambios)
    if faltantes:
        print(f"aviso: faltan metadata.json locales para {len(faltantes)} datasets")

    guardar_local(capas, datasets, paquetes)

    if not args.skip_archive:
        directorio = leer_directorio()
        errores = actualizar_archive_org(
            paquetes,
            capas,
            datasets,
            catalogo,
            directorio,
            cambios_archive,
        )
        if errores:
            print(f"errores Archive.org: {len(errores)}")
            return 1
        if not cambios_archive:
            return 2
    else:
        actualizar_paquetes(paquetes, catalogo, cambios)
        guardar_local(capas, datasets, paquetes)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

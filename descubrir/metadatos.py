#!/usr/bin/env python3

import json
from collections.abc import Callable

import pandas as pd


GEOBOLIVIA_URL = (
    "https://geobolivia.planificacion.gob.bo/"
    "wp-content/themes/geobolivia/assets/data/catalog.json"
)
GEOINFO_URL = "https://infosipeb.planificacion.gob.bo/api/geovisor/geo-cartographics"
METADATA_COLUMNS = ["geoserver", "nombre", "titulo", "descripcion"]


def texto(valor) -> str:
    return str(valor or "").strip()


def solicitar_json(sesion, url: str):
    respuesta = sesion.request("GET", url)
    if respuesta.status != 200:
        raise RuntimeError(f"{url} devolvio estatus {respuesta.status}")
    try:
        return json.loads(respuesta.data)
    except json.JSONDecodeError as error:
        raise RuntimeError(f"{url} no contiene JSON valido") from error


def tabla_metadatos(geoserver: str, filas: list[dict]) -> pd.DataFrame:
    tabla = pd.DataFrame(filas, columns=["nombre", "titulo", "descripcion"])
    for columna in ["nombre", "titulo", "descripcion"]:
        tabla[columna] = tabla[columna].map(texto)
    tabla = tabla[tabla["nombre"].ne("")].copy()
    tabla.insert(0, "geoserver", geoserver)
    if tabla.empty:
        raise RuntimeError(f"la fuente {geoserver} no contiene metadatos utilizables")
    if tabla.duplicated(["geoserver", "nombre"]).any():
        duplicados = tabla.loc[
            tabla.duplicated(["geoserver", "nombre"], keep=False), "nombre"
        ].tolist()
        raise RuntimeError(f"nombres duplicados en {geoserver}: {duplicados[:5]}")
    return tabla[METADATA_COLUMNS]


def procesar_geobolivia(sesion) -> pd.DataFrame:
    payload = solicitar_json(sesion, GEOBOLIVIA_URL)
    resources = payload.get("resources") if isinstance(payload, dict) else None
    if not isinstance(resources, list):
        raise RuntimeError("catalog.json no contiene una lista resources")

    filas = [
        {
            "nombre": recurso.get("technical_name"),
            "titulo": recurso.get("display_title"),
            "descripcion": recurso.get("abstract"),
        }
        for recurso in resources
        if isinstance(recurso, dict)
        and recurso.get("type") == "capas"
        and recurso.get("resource_type") == "dataset"
        and recurso.get("status") == "active"
    ]
    return tabla_metadatos("planificacion.geobolivia", filas)


def procesar_geoinfo(sesion) -> pd.DataFrame:
    payload = solicitar_json(sesion, GEOINFO_URL)
    if not isinstance(payload, list):
        raise RuntimeError("geo-cartographics no contiene una lista")

    filas = [
        {
            "nombre": f"infospie:{registro.get('layer')}",
            "titulo": registro.get("name"),
            "descripcion": registro.get("description"),
        }
        for registro in payload
        if isinstance(registro, dict)
        and registro.get("estado") is True
        and registro.get("layer")
    ]
    return tabla_metadatos("planificacion.geoinfo", filas)


FUENTES: list[tuple[str, Callable]] = [
    ("planificacion.geobolivia", procesar_geobolivia),
    ("planificacion.geoinfo", procesar_geoinfo),
]


def leer_metadatos(sesion) -> tuple[pd.DataFrame, list[tuple[str, str]]]:
    tablas = []
    errores = []
    for geoserver, procesar in FUENTES:
        try:
            tabla = procesar(sesion)
            tablas.append(tabla)
            print(f"metadatos {geoserver}: {len(tabla)} registros")
        except Exception as error:
            errores.append((geoserver, str(error)))
            print(f"aviso: no se pudo leer metadatos {geoserver}: {error}")

    if tablas:
        return pd.concat(tablas, ignore_index=True)[METADATA_COLUMNS], errores
    if errores:
        raise RuntimeError("no se pudo leer ninguna fuente de metadatos")
    return pd.DataFrame(columns=METADATA_COLUMNS), errores


def enriquecer_capas(capas: pd.DataFrame, metadatos: pd.DataFrame) -> pd.DataFrame:
    if metadatos.empty:
        return capas

    salida = capas.copy()
    mapa = metadatos.set_index(["geoserver", "nombre"])
    for indice in salida.index:
        clave = (salida.at[indice, "geoserver"], salida.at[indice, "nombre"])
        if clave not in mapa.index:
            continue
        entrada = mapa.loc[clave]
        if entrada["titulo"]:
            salida.at[indice, "titulo"] = entrada["titulo"]
        if entrada["descripcion"]:
            salida.at[indice, "descripcion"] = entrada["descripcion"]
    return salida

#!/usr/bin/env python3

import json

import pandas as pd


CATALOG_URL = (
    "https://geobolivia.planificacion.gob.bo/"
    "wp-content/themes/geobolivia/assets/data/catalog.json"
)
GEOBOLIVIA_GEOSERVER = "planificacion.geobolivia"
CATALOG_COLUMNS = ["technical_name", "display_title", "abstract"]


def leer_catalogo_geobolivia(sesion) -> tuple[pd.DataFrame, str]:
    respuesta = sesion.request("GET", CATALOG_URL)
    if respuesta.status != 200:
        raise RuntimeError(f"catalog.json devolvio estatus {respuesta.status}")

    try:
        payload = json.loads(respuesta.data)
    except json.JSONDecodeError as error:
        raise RuntimeError("catalog.json no contiene JSON valido") from error

    resources = payload.get("resources")
    if not isinstance(resources, list):
        raise RuntimeError("catalog.json no contiene una lista resources")

    filas = [
        {
            columna: str(recurso.get(columna) or "").strip()
            for columna in CATALOG_COLUMNS
        }
        for recurso in resources
        if recurso.get("type") == "capas"
        and recurso.get("resource_type") == "dataset"
        and recurso.get("status") == "active"
    ]
    catalogo = pd.DataFrame(filas, columns=CATALOG_COLUMNS)
    catalogo = catalogo[catalogo["technical_name"].ne("")].copy()

    if catalogo.empty:
        raise RuntimeError("catalog.json no contiene datasets activos")
    if catalogo["technical_name"].duplicated().any():
        duplicados = catalogo.loc[
            catalogo["technical_name"].duplicated(keep=False), "technical_name"
        ].tolist()
        raise RuntimeError(f"technical_name duplicado en catalog.json: {duplicados[:5]}")

    return catalogo, str(payload.get("generated_at") or "")


def enriquecer_capas_geobolivia(capas: pd.DataFrame, catalogo: pd.DataFrame) -> pd.DataFrame:
    """Aplica títulos y descripciones autoritativos del catálogo a capas activas."""
    if catalogo.empty:
        return capas

    salida = capas.copy()
    mapa = catalogo.set_index("technical_name")
    mascara = salida["geoserver"].eq(GEOBOLIVIA_GEOSERVER)
    for indice in salida.index[mascara & salida["nombre"].isin(mapa.index)]:
        entrada = mapa.loc[salida.at[indice, "nombre"]]
        if entrada["display_title"]:
            salida.at[indice, "titulo"] = entrada["display_title"]
        if entrada["abstract"]:
            salida.at[indice, "descripcion"] = entrada["abstract"]
    return salida

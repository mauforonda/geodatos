#!/usr/bin/env python3
"""
Prueba un geoserver candidato y lo agrega a directorio.json.

Uso:
    python probar_candidato.py https://host/geoserver/ [--nombre N] [--descripcion D] [--inseguro] [--si]

Pasos:
1. Deriva el endpoint ows desde la ruta base.
2. Consulta getCapabilities de wms y wfs y cuenta las capas.
3. Si algún servicio responde con capas, pide nombre y descripción
   y agrega la entrada a directorio.json.
4. Imprime enlaces para revisar las capas descubiertas.
"""

import argparse
import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import quote, urlencode

import urllib3

DIRECTORIO = Path(__file__).resolve().parent / "directorio.json"
VERSION = dict(wms="1.3.0", wfs="1.0.0")


def derivar_ows(base: str) -> str:
    """Deriva el endpoint ows desde la ruta base del geoserver."""
    base = base.strip().rstrip("/")
    for sufijo in ("/ows", "/wms", "/wfs"):
        if base.endswith(sufijo):
            base = base[: -len(sufijo)]
    return base + "/ows"


def local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def listar_capas(ows: str, servicio: str, http) -> list[str] | str:
    """Devuelve los nombres de capas o un texto con el error."""
    url = f"{ows}?" + urlencode(
        dict(service=servicio, version=VERSION[servicio.lower()], request="GetCapabilities")
    )
    try:
        r = http.request("GET", url)
    except Exception as e:
        return f"{type(e).__name__}: {e}"
    if r.status != 200:
        return f"HTTP {r.status}"
    try:
        raiz = ET.fromstring(r.data)
    except ET.ParseError:
        return "respuesta no es XML"
    objetivo = "Layer" if servicio.lower() == "wms" else "FeatureType"
    nombres = []
    for el in raiz.iter():
        if local(el.tag) == objetivo:
            for hijo in el:
                if local(hijo.tag) == "Name" and hijo.text:
                    nombres.append(hijo.text.strip())
    if not nombres:
        return "sin capas (¿ServiceException?)"
    return nombres


def enlace_wms(base: str, capa: str) -> str:
    c = quote(capa, safe=":")
    return (
        f"{base.rstrip('/')}/wms/reflect?layers={c}"
        "&width=1000&height=1000&format=application/openlayers"
    )


def cargar() -> list:
    return json.loads(DIRECTORIO.read_text(encoding="utf-8"))


def guardar(entradas: list) -> None:
    DIRECTORIO.write_text(
        json.dumps(entradas, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__.split("\n")[1])
    p.add_argument("base", help="ruta base, ej. https://host/geoserver/")
    p.add_argument("--nombre")
    p.add_argument("--descripcion")
    p.add_argument(
        "--inseguro",
        action="store_true",
        help="no verificar certificados TLS (servidores con certificado inválido)",
    )
    p.add_argument("--si", action="store_true", help="no pedir confirmación")
    a = p.parse_args()

    ows = derivar_ows(a.base)
    base = ows[: -len("/ows")]
    print(f"ows: {ows}")

    existente = next((e for e in cargar() if e["ows"].rstrip("/") == ows), None)
    if existente:
        print(f"Ya existe en el directorio como '{existente['nombre']}'.")
        return 1

    if a.inseguro:
        urllib3.disable_warnings()
    http = urllib3.PoolManager(
        timeout=30, retries=1, cert_reqs="CERT_NONE" if a.inseguro else "CERT_REQUIRED"
    )

    capas = {}
    for servicio in ("wms", "wfs"):
        res = listar_capas(ows, servicio.upper(), http)
        if isinstance(res, str):
            print(f"{servicio}: NO ({res})")
            capas[servicio] = []
        else:
            print(f"{servicio}: SÍ, {len(res)} capas")
            capas[servicio] = res

    if not any(capas.values()):
        print("Ningún servicio responde. No se agrega.")
        return 2

    nombre = a.nombre or input("nombre (ej. entidad.sistema): ").strip()
    descripcion = a.descripcion or input("descripcion: ").strip()
    if not nombre or not descripcion:
        print("Nombre y descripción son obligatorios.")
        return 3
    entradas = cargar()
    if any(e["nombre"] == nombre for e in entradas):
        print(f"El nombre '{nombre}' ya existe.")
        return 1

    entrada = dict(
        ows=ows,
        nombre=nombre,
        descripcion=descripcion,
        wms=bool(capas["wms"]),
        wfs=bool(capas["wfs"]),
    )
    print(json.dumps(entrada, indent=2, ensure_ascii=False))
    if not a.si and input("¿Agregar a directorio.json? [s/N] ").strip().lower() != "s":
        print("Cancelado.")
        return 0
    entradas.append(entrada)
    guardar(entradas)
    print(f"Agregado a {DIRECTORIO}")

    print("\nEnlaces para revisar capas wms:")
    for capa in capas["wms"]:
        print(enlace_wms(base, capa))
    return 0


if __name__ == "__main__":
    sys.exit(main())

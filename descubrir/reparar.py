#!/usr/bin/env python3

import concurrent.futures
import json
from pathlib import Path
from urllib.parse import urlencode
import xml.etree.ElementTree as ET

import requests
import urllib3


BASE_DIR = Path(__file__).resolve().parent
DIRECTORIO = BASE_DIR / "directorio.json"

TIMEOUT = (5, 20)
MAX_WORKERS = 8

WMS_VERSIONS = [None, "1.3.0", "1.1.1"]
WFS_VERSIONS = [None, "2.0.0", "1.1.0", "1.0.0"]


def load_directorio() -> list[dict]:
    with open(DIRECTORIO, encoding="utf-8") as f:
        return json.load(f)


def save_directorio(data: list[dict]) -> None:
    with open(DIRECTORIO, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
        f.write("\n")


def build_url(base_url: str, params: dict[str, str]) -> str:
    separator = "&" if "?" in base_url else "?"
    return f"{base_url}{separator}{urlencode(params)}"


def root_name(xml_bytes: bytes) -> str | None:
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return None
    return root.tag.rsplit("}", 1)[-1]


def is_capabilities(service: str, xml_bytes: bytes) -> bool:
    root = root_name(xml_bytes)
    if root is None:
        return False
    if service == "WMS":
        return root in {"WMS_Capabilities", "WMT_MS_Capabilities"}
    if service == "WFS":
        return root == "WFS_Capabilities"
    return False


def probe_service(session: requests.Session, ows: str, service: str) -> bool:
    versions = WMS_VERSIONS if service == "WMS" else WFS_VERSIONS
    for version in versions:
        params = {"service": service, "request": "GetCapabilities"}
        if version is not None:
            params["version"] = version
        url = build_url(ows, params)
        try:
            response = session.get(url, timeout=TIMEOUT)
        except requests.RequestException:
            continue
        if response.status_code != 200:
            continue
        if is_capabilities(service, response.content):
            return True
    return False


def repair_entry(entry: dict) -> tuple[str, list[str]]:
    session = requests.Session()
    session.headers.update({"User-Agent": "geodatos-repair/1.0"})
    session.verify = False

    nombre = entry.get("nombre", "")
    updated = []

    if entry.get("wms") is False and probe_service(session, entry["ows"], "WMS"):
        entry["wms"] = True
        updated.append("wms")

    if entry.get("wfs") is False and probe_service(session, entry["ows"], "WFS"):
        entry["wfs"] = True
        updated.append("wfs")

    return nombre, updated


def main() -> int:
    urllib3.disable_warnings()
    directorio = load_directorio()

    targets = [entry for entry in directorio if entry.get("wms") is False or entry.get("wfs") is False]
    if not targets:
        print("reparar: no hay interfaces marcadas como false")
        return 0

    resultados: list[tuple[str, list[str]]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(MAX_WORKERS, len(targets))) as executor:
        for resultado in executor.map(repair_entry, targets):
            resultados.append(resultado)

    cambios = [(nombre, campos) for nombre, campos in resultados if campos]
    if not cambios:
        print(f"reparar: revisadas {len(targets)} fuentes, sin cambios")
        return 0

    save_directorio(directorio)

    total = sum(len(campos) for _, campos in cambios)
    print(f"reparar: {total} interfaces reactivadas en {len(cambios)} fuentes")
    for nombre, campos in cambios:
        print(f"- {nombre}: {', '.join(campos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

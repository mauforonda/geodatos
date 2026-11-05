#!/usr/bin/env python3

"""Normaliza los geoparquets ya archivados en Archive.org y les agrega teselas PMTiles.

Un geoparquet normalizado es GeoParquet 1.1, EPSG:4326, columna `bbox` con
metadatos de covering, filas en orden Hilbert, filas por grupo acotadas y
compresion ZSTD. Es el mismo formato que escribe `escribir_geoparquet` al
archivar. Cada dataset se descarga, se normaliza, se verifica contra el
original, se perfila, se le agregan teselas (`dataset.pmtiles`), se vuelve a
subir al mismo item junto con su `datapackage.json` y se marca `normalizado`
y `bytes_pmtiles` en `paquetes.csv`. Tambien escribe la coleccion STAC del
dataset y reconstruye los catalogos de su fuente.

Los datasets que no se pueden normalizar por sus datos se anotan en
`normalizar_omitidos.csv` con su tipo y no se vuelven a intentar:

- `crs`: las coordenadas no corresponden al sistema que declara la fuente
  (p. ej. metros declarados como EPSG:4326).
- `geometria`: la conversion falla por las geometrias, que no se reparan.
- `pmtiles`: el dataset queda normalizado pero no se puede teselar (sin
  geometrias, demasiado grande o falla el teselado).
"""

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Optional

import urllib3
from geoparquet_io.core.reproject import reproject_impl

ROOT_DIR = Path(__file__).resolve().parent.parent
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from archivar.actualizar import (  # noqa: E402
    OMITIDOS,
    PAQUETES,
    archive_download_url,
    convertir_a_geoparquet,
    escribir_json,
    guardar_paquetes,
    intentar_pmtiles,
    iniciar_sesion,
    leer_omitidos,
    leer_paquetes,
    registrar_omitido,
    rutas_publicacion,
    texto_o_vacio,
)
from archivar.crs import extensiones_validas_por_geoserver, inferir_crs, limites_de_geoparquet  # noqa: E402
from archivar.geoparquet import (  # noqa: E402
    bbox_lonlat_valido,
    esta_normalizado,
    inspeccionar,
    suma_hashes,
)
from archivar.paquete_datos import construir_datapackage, perfilar  # noqa: E402
from archivar.pmtiles import ARCHIVO as ARCHIVO_PMTILES  # noqa: E402
from archivar.stac import actualizar_catalogos, construir_collection  # noqa: E402

ARCHIVO = "dataset.geoparquet"
MAX_MINUTOS_POR_DATASET = 60
DESCARGA_CHUNK = 1024 * 1024


class NormalizacionError(RuntimeError):
    pass


class DatosInconsistentesError(NormalizacionError):
    """Los datos no se pueden normalizar y reintentar no cambia el resultado."""

    def __init__(self, tipo: str, motivo: str):
        super().__init__(motivo)
        self.tipo = tipo


def conversion_falla_por_geometria(error: Exception) -> bool:
    texto = str(error)
    return any(marca in texto for marca in ("Invalid Input Error", "IllegalArgument", "TopologyException", "GEOS"))


def hash_atributos(path: Path, info: dict) -> tuple[int, str]:
    columnas = [c for c in info["columnas"] if c not in {info["geometria"], "bbox"}]
    return suma_hashes(path, columnas)


def normalizar_archivo(
    origen: Path, destino: Path, epsg_declarado: Optional[int] = None, hermanas=None
) -> dict:
    """Escribe `destino` normalizado desde `origen` y verifica que no pierde datos."""
    info = inspeccionar(origen)
    crs_inferido = None
    # Declara WGS84 o no declara nada, pero las coordenadas no son lon/lat
    if (info["crs_4326"] or info["crs"] is None) and info["bbox_geo"] and not bbox_lonlat_valido(info["bbox_geo"]):
        preferidos = [f"EPSG:{epsg_declarado}"] if epsg_declarado and epsg_declarado != 4326 else None
        crs_inferido, plausibles = inferir_crs(limites_de_geoparquet(origen, info["geometria"]), preferidos, hermanas)
        if crs_inferido is None:
            raise DatosInconsistentesError(
                "crs",
                f"coordenadas fuera de lon/lat {info['bbox_geo']} y ningun CRS candidato encaja de forma unica "
                f"(plausibles: {', '.join(plausibles) or 'ninguno'})",
            )
    with tempfile.TemporaryDirectory(prefix="geodatos-normalizar-") as tmpdir:
        entrada = origen
        if crs_inferido or not info["crs_4326"]:
            reproyectado = Path(tmpdir) / "reproyectado.parquet"
            argumentos = {}
            if crs_inferido:
                argumentos["source_crs"] = crs_inferido
            elif info["crs"] is None:
                # CRS declarado como null y coordenadas lon/lat
                argumentos["assume_crs84"] = True
            reproject_impl(
                str(origen),
                str(reproyectado),
                target_crs="EPSG:4326",
                geoparquet_version="1.1",
                **argumentos,
            )
            entrada = reproyectado
        try:
            convertir_a_geoparquet(str(entrada), str(destino))
        except Exception as e:
            if conversion_falla_por_geometria(e):
                raise DatosInconsistentesError("geometria", str(e)) from e
            raise

    nuevo = inspeccionar(destino)
    if not esta_normalizado(nuevo):
        raise NormalizacionError(f"la salida no cumple el formato: {nuevo}")
    if not bbox_lonlat_valido(nuevo["bbox_geo"] or []):
        raise DatosInconsistentesError("crs", f"bbox fuera del rango lon/lat: {nuevo['bbox_geo']}")
    if info["crs_4326"] and not crs_inferido:
        antes, despues = hash_atributos(origen, info), hash_atributos(destino, nuevo)
    else:
        # Tras reproyectar la geometria cambia; se comparan solo las filas
        antes, despues = (info["filas"], ""), (nuevo["filas"], "")
    if antes != despues:
        raise NormalizacionError(f"la salida difiere del original: {antes} != {despues}")
    return {"antes": info, "despues": nuevo, "crs_inferido": crs_inferido}


def descargar_archivo(sesion: urllib3.PoolManager, item: str, destino: Path) -> None:
    respuesta = sesion.request("GET", archive_download_url(item, ARCHIVO), preload_content=False)
    try:
        if respuesta.status != 200:
            raise NormalizacionError(f"Archive.org devolvio estatus {respuesta.status} para {item}")
        esperado = respuesta.headers.get("Content-Length")
        with open(destino, "wb") as f:
            for chunk in respuesta.stream(DESCARGA_CHUNK):
                f.write(chunk)
    finally:
        respuesta.release_conn()
    if esperado and destino.stat().st_size != int(esperado):
        raise NormalizacionError(f"descarga incompleta de {item}")


def subir_archivos(item: str, archivos: dict[str, Path]) -> None:
    from internetarchive import upload

    access_key = os.environ.get("IA_ACCESS_KEY")
    secret_key = os.environ.get("IA_SECRET_KEY")
    if not access_key or not secret_key:
        raise RuntimeError("Faltan IA_ACCESS_KEY y/o IA_SECRET_KEY en el entorno")

    resultado = upload(
        item,
        {nombre: str(path) for nombre, path in archivos.items()},
        access_key=access_key,
        secret_key=secret_key,
        verify=False,
        verbose=True,
    )
    if not all(r.status_code in [200, 302] for r in resultado):
        raise RuntimeError(f"La subida a Archive.org no completo correctamente para {item}")


def marcar_normalizado(
    geoserver: str, nombre: str, bytes_geoparquet: int, hash_contenido: str, bytes_pmtiles: "int | str"
) -> None:
    # Se relee para no pisar cambios hechos por otro proceso desde que empezo el dataset
    paquetes = leer_paquetes()
    mascara = (paquetes["geoserver"] == geoserver) & (paquetes["nombre"] == nombre)
    paquetes.loc[mascara, "normalizado"] = True
    paquetes.loc[mascara, "bytes_geoparquet"] = bytes_geoparquet
    paquetes.loc[mascara, "hash"] = hash_contenido
    paquetes.loc[mascara, "bytes_pmtiles"] = bytes_pmtiles
    guardar_paquetes(paquetes)


def git(*args: str) -> subprocess.CompletedProcess:
    return subprocess.run(["git", *args], cwd=ROOT_DIR, check=True, capture_output=True, text=True)


def guardar_checkpoint(mensaje: str, rutas: list[Path]) -> None:
    # "[skip ci]": los push con GITHUB_TOKEN ya no disparan workflows; esto cubre un push con otro token
    git("add", *[str(r.relative_to(ROOT_DIR)) for r in rutas if r.exists()])
    if subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=ROOT_DIR).returncode == 0:
        return
    git("commit", "-m", f"{mensaje} [skip ci]")
    for _ in range(3):
        try:
            git("push", "origin", "HEAD:master")
            return
        except subprocess.CalledProcessError as e:
            print(f"aviso: push fallo ({e.stderr.strip()}); reintentando")
            git("pull", "--rebase", "origin", "master")
    git("push", "origin", "HEAD:master")


def leer_metadata(fila, rutas: dict[str, Path]) -> dict:
    if rutas["metadata"].exists():
        with open(rutas["metadata"], encoding="utf-8") as f:
            return json.load(f)
    return {
        "geoserver": fila.geoserver,
        "nombre": fila.nombre,
        "titulo": fila.titulo,
        "descripcion": fila.descripcion,
        "fecha_archivado": fila.fecha_archivado,
        "archive_item": fila.archive_item,
    }


def normalizar_uno(
    sesion: urllib3.PoolManager,
    fila,
    dry_run: bool,
    max_minutos: int,
    salida_prueba: Optional[Path],
) -> tuple[str, list[Path]]:
    item = texto_o_vacio(fila.archive_item)
    epsg = int(fila.epsg) if str(fila.epsg).strip() not in {"", "nan"} else None

    def timeout(signum, frame):
        raise TimeoutError(f"timeout normalizando {item} despues de {max_minutos} minutos")

    previo = signal.signal(signal.SIGALRM, timeout)
    signal.alarm(max_minutos * 60)
    try:
        with tempfile.TemporaryDirectory(prefix="geodatos-normalizar-") as tmpdir:
            tmp = Path(tmpdir)
            original = tmp / "original.parquet"
            salida = tmp / ARCHIVO
            descargar_archivo(sesion, item, original)

            crs_inferido = None
            if esta_normalizado(inspeccionar(original)):
                final, estado, subir_parquet = original, "ya_normalizado", False
            else:
                hermanas = extensiones_validas_por_geoserver().get(fila.geoserver)
                crs_inferido = normalizar_archivo(original, salida, epsg, hermanas)["crs_inferido"]
                final, estado, subir_parquet = salida, "normalizado", True
            print(
                f"{item}: {estado}, {final.stat().st_size} bytes (original {original.stat().st_size})"
                + (f", CRS inferido {crs_inferido}" if crs_inferido else ""),
                flush=True,
            )

            perfil = perfilar(final, crs_inferido)
            rutas = rutas_publicacion(fila.geoserver, fila.nombre)
            metadata = leer_metadata(fila, rutas)
            datapackage = construir_datapackage(metadata, perfil)
            pmtiles_path = tmp / ARCHIVO_PMTILES
            pmtiles, omision_pmtiles = intentar_pmtiles(final, pmtiles_path)
            if omision_pmtiles:
                print(f"{item}: sin PMTiles ({omision_pmtiles})", flush=True)
            coleccion = construir_collection(metadata, perfil, pmtiles)

            if dry_run:
                if salida_prueba:
                    escribir_json(salida_prueba / f"{item}.datapackage.json", datapackage)
                    escribir_json(salida_prueba / f"{item}.collection.json", coleccion)
                    if pmtiles:
                        shutil.copy2(pmtiles_path, salida_prueba / f"{item}.pmtiles")
                return estado, []

            rutas["base"].mkdir(parents=True, exist_ok=True)
            escribir_json(rutas["datapackage"], datapackage)
            escribir_json(rutas["collection"], coleccion)
            archivos = {"datapackage.json": rutas["datapackage"]}
            if subir_parquet:
                archivos[ARCHIVO] = final
            if pmtiles:
                archivos[ARCHIVO_PMTILES] = pmtiles_path
            subir_archivos(item, archivos)
            marcar_normalizado(
                fila.geoserver, fila.nombre, final.stat().st_size, perfil["hash"], pmtiles["bytes"] if pmtiles else ""
            )
            guardados = [PAQUETES, rutas["datapackage"], rutas["collection"]]
            guardados += actualizar_catalogos(rutas["base"].parent)
            if omision_pmtiles:
                registrar_omitido(fila.geoserver, fila.nombre, "pmtiles", omision_pmtiles)
                guardados.append(OMITIDOS)
            return estado, guardados
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previo)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-datasets", type=int, default=None)
    parser.add_argument("--max-minutes", type=int, default=100)
    parser.add_argument("--max-minutes-por-dataset", type=int, default=MAX_MINUTOS_POR_DATASET)
    parser.add_argument("--item", action="append", default=[], help="Solo estos archive_item")
    parser.add_argument("--dry-run", action="store_true", help="No sube ni modifica el repositorio")
    parser.add_argument("--salida-prueba", type=Path, default=None, help="Con --dry-run: carpeta para los datapackage.json")
    parser.add_argument("--commit", action="store_true", help="Commit y push tras cada dataset")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    paquetes = leer_paquetes()
    omitidos = leer_omitidos()
    sin_pmtiles = paquetes["bytes_pmtiles"].astype(str).isin(["", "nan"])
    pendientes = paquetes[(~paquetes["normalizado"] | sin_pmtiles) & paquetes["archive_item"].astype(str).ne("")]
    pendientes = pendientes[
        [(g, n) not in omitidos for g, n in zip(pendientes["geoserver"], pendientes["nombre"], strict=True)]
    ]
    if args.item:
        pendientes = pendientes[pendientes["archive_item"].isin(args.item)]
    # Primero los livianos: cada corrida avanza sobre la mayor cantidad de datasets
    pendientes = pendientes.assign(
        _bytes=pendientes["bytes_geoparquet"].replace("", 0).astype(float)
    ).sort_values("_bytes", kind="mergesort")
    if pendientes.empty:
        print("No quedan geoparquets por normalizar.")
        return 2
    if args.salida_prueba:
        args.salida_prueba.mkdir(parents=True, exist_ok=True)

    sesion = iniciar_sesion()
    inicio = time.monotonic()
    normalizados = fallidos = omitidos_ahora = 0
    for fila in pendientes.itertuples(index=False):
        if args.max_datasets is not None and normalizados + fallidos + omitidos_ahora >= args.max_datasets:
            break
        if (time.monotonic() - inicio) / 60 >= args.max_minutes:
            print("Se alcanzo el limite de tiempo.")
            break
        print(f"normalizando {fila.geoserver} / {fila.nombre}", flush=True)
        try:
            _, rutas = normalizar_uno(
                sesion, fila, args.dry_run, args.max_minutes_por_dataset, args.salida_prueba
            )
        except DatosInconsistentesError as e:
            omitidos_ahora += 1
            print(f"omitido ({e.tipo}): {fila.geoserver} / {fila.nombre}: {e}", flush=True)
            if not args.dry_run:
                registrar_omitido(fila.geoserver, fila.nombre, e.tipo, str(e))
                if args.commit:
                    guardar_checkpoint(f"[normalizar] omitir {fila.geoserver} {fila.nombre}"[:200], [OMITIDOS])
            continue
        except Exception as e:
            fallidos += 1
            print(f"error: {fila.geoserver} / {fila.nombre}: {e}", flush=True)
            continue
        normalizados += 1
        if args.commit and not args.dry_run:
            guardar_checkpoint(f"[normalizar] {fila.geoserver} {fila.nombre}"[:200], rutas)

    print(
        f"normalizados: {normalizados}, omitidos: {omitidos_ahora}, fallidos: {fallidos}, "
        f"pendientes al inicio: {len(pendientes)}"
    )
    return 1 if normalizados == 0 and fallidos > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())

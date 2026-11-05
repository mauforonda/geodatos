import geopandas as gpd
import pytest
from shapely.geometry import Point

from archivar.geoparquet import esta_normalizado, inspeccionar
from archivar.normalizar import (
    DatosInconsistentesError,
    normalizar_archivo,
)
from archivar.paquete_datos import construir_datapackage, perfilar


def geoparquet_legado(path, crs, n=200, grados=False):
    # Escritura sin bbox, sin orden espacial y con Snappy, como los archivos antiguos
    gdf = gpd.GeoDataFrame(
        {
            "id": range(n),
            "nombre": [f"p{i}" for i in range(n)],
            "tipo": ["a" if i % 3 else "b" for i in range(n)],
            "valor": [i * 0.5 for i in range(n)],
        },
        geometry=[
            Point(-68 + ((i * 7919) % 9000) / 1000, -17 + ((i * 104729) % 9000) / 1000)
            if grados
            else Point(500000 + (i * 7919) % 9000, 8000000 + (i * 104729) % 9000)
            for i in range(n)
        ],
        crs=crs,
    )
    gdf.to_parquet(path, index=False, compression="snappy")
    return gdf


def test_normaliza_utm_a_4326(tmp_path):
    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, "EPSG:32719")
    assert not esta_normalizado(inspeccionar(origen))

    destino = tmp_path / "dataset.geoparquet"
    normalizar_archivo(origen, destino)

    info = inspeccionar(destino)
    assert esta_normalizado(info)
    assert info["filas"] == 200
    crs = gpd.read_parquet(destino).crs
    assert crs.is_geographic and crs.axis_info[0].direction == "east"


def test_normaliza_4326_conserva_atributos(tmp_path):
    origen = tmp_path / "origen.parquet"
    original = geoparquet_legado(origen, "EPSG:4326", grados=True)
    destino = tmp_path / "dataset.geoparquet"
    normalizar_archivo(origen, destino)

    nuevo = gpd.read_parquet(destino).sort_values("id").reset_index(drop=True)
    assert list(nuevo["nombre"]) == list(original["nombre"])
    assert esta_normalizado(inspeccionar(destino))


def test_metros_ambiguos_entre_zonas_utm_se_omiten(tmp_path):
    # Estas coordenadas caen dentro de Bolivia tanto en la zona 19S como en la 20S
    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, "EPSG:4326")  # metros etiquetados como grados
    with pytest.raises(DatosInconsistentesError) as e:
        normalizar_archivo(origen, tmp_path / "dataset.geoparquet")
    assert e.value.tipo == "crs" and "EPSG:32719" in str(e.value) and "EPSG:32720" in str(e.value)


def test_metros_sin_candidato_se_omiten(tmp_path):
    origen = tmp_path / "origen.parquet"
    gdf = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[Point(1_200_000, 900_000), Point(1_300_000, 950_000)], crs="EPSG:4326")
    gdf.to_parquet(origen, index=False)
    with pytest.raises(DatosInconsistentesError) as e:
        normalizar_archivo(origen, tmp_path / "dataset.geoparquet")
    assert "ninguno" in str(e.value)


def test_infiere_crs_unico_y_reproyecta(tmp_path):
    from pyproj import Transformer

    # Sur de Potosi: solo la zona 19S ubica estas coordenadas dentro de Bolivia
    x, y = Transformer.from_crs("EPSG:4326", "EPSG:32719", always_xy=True).transform(
        [-67.5, -67.3, -67.4], [-21.5, -21.4, -21.6]
    )
    origen = tmp_path / "origen.parquet"
    gdf = gpd.GeoDataFrame({"id": [1, 2, 3]}, geometry=[Point(a, b) for a, b in zip(x, y, strict=True)], crs="EPSG:4326")
    gdf.to_parquet(origen, index=False)
    destino = tmp_path / "dataset.geoparquet"
    resultado = normalizar_archivo(origen, destino)

    assert resultado["crs_inferido"] == "EPSG:32719"
    salida = gpd.read_parquet(destino)
    assert abs(salida.geometry.x.mean() + 67.4) < 0.01 and abs(salida.geometry.y.mean() + 21.5) < 0.01
    campos = {c["name"]: c for c in perfilar(destino, "EPSG:32719")["campos"]}
    assert "EPSG:32719" in campos["geometry"]["description"]


def test_crs_nulo_prefiere_el_epsg_declarado(tmp_path):
    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, None)  # encaja en 19S y en 20S: gana el que declara la fuente
    destino = tmp_path / "dataset.geoparquet"
    resultado = normalizar_archivo(origen, destino, epsg_declarado=32719)
    assert resultado["crs_inferido"] == "EPSG:32719"
    assert esta_normalizado(inspeccionar(destino))


def test_datapackage_y_hash(tmp_path):
    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, "EPSG:4326", grados=True)
    a, b = tmp_path / "a.geoparquet", tmp_path / "b.geoparquet"
    normalizar_archivo(origen, a)
    normalizar_archivo(origen, b)

    perfil = perfilar(a)
    assert perfil["hash"] == perfilar(b)["hash"]
    assert perfil["coordenadas_lonlat"] is True

    metadata = {
        "geoserver": "demo.geo",
        "fuente": "Demo",
        "ows": "https://example.org/geoserver/ows",
        "nombre": "ws:capa",
        "titulo": "Capa de prueba",
        "descripcion": "",
        "fecha_archivado": "2026-01-02",
        "archive_item": "geodatosbolivia_demo_geo_ws_capa",
    }
    paquete = construir_datapackage(metadata, perfil)
    assert set(paquete) == {"$schema", "name", "title", "sources", "resources"}
    recurso = paquete["resources"][0]
    assert recurso["hash"].startswith("sha256:") and recurso["bytes"] == a.stat().st_size
    campos = {c["name"]: c for c in recurso["schema"]["fields"]}
    assert list(campos) == inspeccionar(a)["columnas"]
    assert campos["bbox"]["type"] == "object"
    assert "Point" in campos["geometry"]["description"]
    assert campos["id"]["type"] == "integer" and campos["id"]["constraints"]["unique"] is True
    assert campos["valor"]["constraints"]["maximum"] == 99.5
    assert campos["tipo"]["categories"] == ["a", "b"]  # de la mas a la menos frecuente


def test_hash_distingue_contenido(tmp_path):
    origen = tmp_path / "origen.parquet"
    gdf = geoparquet_legado(origen, "EPSG:4326", grados=True)
    gdf.loc[0, "nombre"] = "otro"
    cambiado = tmp_path / "cambiado.parquet"
    gdf.to_parquet(cambiado, index=False)
    a, b = tmp_path / "a.geoparquet", tmp_path / "b.geoparquet"
    normalizar_archivo(origen, a)
    normalizar_archivo(cambiado, b)
    assert perfilar(a)["hash"] != perfilar(b)["hash"]


def test_geometrias_invalidas_se_conservan_y_se_informan(tmp_path):
    from shapely.geometry import Polygon

    origen = tmp_path / "origen.parquet"
    corbata = Polygon([(0, 0), (1, 1), (1, 0), (0, 1), (0, 0)])  # auto-intersecta
    gdf = gpd.GeoDataFrame({"id": [1, 2]}, geometry=[corbata, Polygon([(2, 2), (3, 2), (3, 3)])], crs="EPSG:4326")
    gdf.to_parquet(origen, index=False)
    destino = tmp_path / "dataset.geoparquet"
    normalizar_archivo(origen, destino)

    salida = gpd.read_parquet(destino).sort_values("id").reset_index(drop=True)
    assert not salida.geometry.iloc[0].is_valid
    assert salida.geometry.iloc[0].equals(corbata)
    campos = {c["name"]: c for c in perfilar(destino)["campos"]}
    assert "inválidas, conservadas sin reparar: 1" in campos["geometry"]["description"]


def stage_falso(tmp_path, monkeypatch, epsg, crs_miembro):
    import json

    import pandas as pd

    from archivar import actualizar

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {"id": i, "nombre": f"p{i}"},
                "geometry": {"type": "Point", "coordinates": [500000 + i * 10, 8000000 + i * 10]},
            }
            for i in range(20)
        ],
    }
    if crs_miembro:
        geojson["crs"] = {"type": "name", "properties": {"name": f"urn:ogc:def:crs:EPSG::{crs_miembro}"}}

    def descargar_falso(sesion, ows, nombre, destino, max_bytes=None):
        destino.write_text(json.dumps(geojson))
        return 20, 20, destino.stat().st_size, False

    monkeypatch.setattr(actualizar, "descargar_geojson", descargar_falso)
    monkeypatch.setattr(actualizar, "descargar_imagen_opcional", lambda *a, **k: None)
    monkeypatch.setattr(actualizar, "generar_mapa_publicacion", lambda *a, **k: None)
    monkeypatch.setattr(actualizar, "TMP_STAGE", tmp_path / "stage")

    capa = pd.Series(
        {
            "geoserver": "demo.geo",
            "nombre": "ws:utm",
            "titulo": "Capa UTM",
            "descripcion": "",
            "epsg": epsg,
            "min_x": 500000.0,
            "max_x": 500200.0,
            "min_y": 8000000.0,
            "max_y": 8000200.0,
        }
    )
    directorio = {"demo.geo": {"ows": "https://example.org/geoserver/ows", "descripcion": "Demo"}}
    return actualizar.stage_dataset(None, directorio, capa, map_mode="none", max_bytes_descarga=None)


def test_stage_dataset_escribe_geoparquet_normalizado_y_datapackage(tmp_path, monkeypatch):
    import json

    staged = stage_falso(tmp_path, monkeypatch, epsg=32719, crs_miembro=32719)

    info = inspeccionar(staged["geoparquet_path"])
    assert esta_normalizado(info) and staged["normalizado"] is True
    assert -70 < info["bbox_geo"][0] < -60
    paquete = json.loads(staged["datapackage_path"].read_text())
    assert staged["hash"] and len(staged["hash"]) == 32
    assert paquete["resources"][0]["bytes"] == staged["geoparquet_path"].stat().st_size
    assert staged["collection_path"].exists() and staged["pmtiles_path"].exists()
    assert staged["bytes_pmtiles"] == staged["pmtiles_path"].stat().st_size


def test_stage_dataset_con_metros_ambiguos_declarados_como_4326_no_se_marca_normalizado(tmp_path, monkeypatch):
    import json

    staged = stage_falso(tmp_path, monkeypatch, epsg=4326, crs_miembro=None)

    assert staged["normalizado"] is False
    assert not staged["collection_path"].exists() and not staged["pmtiles_path"].exists()
    assert staged["bytes_pmtiles"] == ""
    campos = json.loads(staged["datapackage_path"].read_text())["resources"][0]["schema"]["fields"]
    descripcion = next(c for c in campos if c["name"] == "geometry")["description"]
    assert "Atención" in descripcion


def test_omitidos_registra_tipo_y_se_excluyen(tmp_path, monkeypatch):
    from archivar import actualizar

    monkeypatch.setattr(actualizar, "OMITIDOS", tmp_path / "omitidos.csv")
    assert actualizar.leer_omitidos() == set()
    actualizar.registrar_omitido("a.geo", "ws:uno", "crs", "metros\ndeclarados como 4326")
    actualizar.registrar_omitido("a.geo", "ws:dos", "geometria", "falla la conversion")
    assert actualizar.leer_omitidos() == {("a.geo", "ws:uno"), ("a.geo", "ws:dos")}
    lineas = (tmp_path / "omitidos.csv").read_text().splitlines()
    assert lineas[0] == "geoserver,nombre,fecha,tipo,motivo"
    assert ",crs," in lineas[1] and ",geometria," in lineas[2]


def caja_lonlat(crs, origen):
    import numpy as np
    from pyproj import Transformer

    gdf = gpd.read_parquet(origen)
    lon, lat = Transformer.from_crs(crs, "EPSG:4326", always_xy=True).transform(
        gdf.geometry.x.to_numpy(), gdf.geometry.y.to_numpy()
    )
    return np.array([[lon.min(), lat.min(), lon.max(), lat.max()]])


def test_extensiones_de_capas_hermanas_desempatan_entre_zonas_utm(tmp_path):
    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, "EPSG:4326")  # encaja en 19S y en 20S

    sin_hermanas = tmp_path / "a.geoparquet"
    with pytest.raises(DatosInconsistentesError):
        normalizar_archivo(origen, sin_hermanas)

    for zona, esperado in [("EPSG:32719", "EPSG:32719"), ("EPSG:32720", "EPSG:32720")]:
        destino = tmp_path / f"{zona[-2:]}.geoparquet"
        resultado = normalizar_archivo(origen, destino, hermanas=caja_lonlat(zona, origen) + 0.005)
        assert resultado["crs_inferido"] == esperado
        assert esta_normalizado(inspeccionar(destino))


def test_hermanas_que_no_coinciden_o_coinciden_con_ambas_no_desempatan(tmp_path):
    import numpy as np

    origen = tmp_path / "origen.parquet"
    geoparquet_legado(origen, "EPSG:4326")
    lejanas = np.array([[-66.0, -20.0, -65.0, -19.0]])
    ambas = np.vstack([caja_lonlat("EPSG:32719", origen), caja_lonlat("EPSG:32720", origen)])
    for hermanas in (lejanas, ambas):
        with pytest.raises(DatosInconsistentesError):
            normalizar_archivo(origen, tmp_path / "x.geoparquet", hermanas=hermanas)


def test_extensiones_validas_por_geoserver_ignora_cajas_fuera_de_bolivia(tmp_path):
    from archivar.crs import extensiones_validas_por_geoserver

    capas = tmp_path / "capas.csv"
    capas.write_text(
        "geoserver,nombre,min_x,min_y,max_x,max_y\n"
        "a.geo,ws:valida,-68.54,-16.42,-67.23,-15.1\n"
        "a.geo,ws:metros,549416.9,8184050.0,689906.9,8330240.0\n"
        "a.geo,ws:mundo,-180,-90,180,90\n"
        "a.geo,ws:vacia,,,,\n"
        "b.geo,ws:otra,-65.0,-17.0,-64.0,-16.0\n"
    )
    extensiones_validas_por_geoserver.cache_clear()
    por_geoserver = extensiones_validas_por_geoserver(capas)
    extensiones_validas_por_geoserver.cache_clear()
    assert por_geoserver["a.geo"].tolist() == [[-68.54, -16.42, -67.23, -15.1]]
    assert por_geoserver["b.geo"].shape == (1, 4)

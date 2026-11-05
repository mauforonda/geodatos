import json

import geopandas as gpd
import pytest
from pmtiles.reader import MmapSource, Reader
from shapely.geometry import GeometryCollection, LineString, Point, Polygon

from archivar import stac
from archivar.normalizar import normalizar_archivo
from archivar.paquete_datos import perfilar
from archivar.pmtiles import (
    PmtilesOmitido,
    construir_pmtiles,
    elegir_propiedades,
    redondear_cifras,
    zoom_maximo,
    zoom_minimo,
)


def normalizado(tmp_path, gdf):
    origen, destino = tmp_path / "origen.parquet", tmp_path / "dataset.geoparquet"
    gdf.to_parquet(origen, index=False)
    normalizar_archivo(origen, destino)
    return destino


def puntos(n=300):
    return gpd.GeoDataFrame(
        {
            "id": range(n),
            "departamento": [("LA PAZ", "ORURO", "BENI")[i % 3] for i in range(n)],
            "unico": [f"u{i}" for i in range(n)],
            "lon": [-68.0 + i / 100 for i in range(n)],
            "poblacion": [1000 + (i * 37) % 997 for i in range(n)],
            "constante": [5] * n,
            "indice": [i * 0.123456 for i in range(n)],
        },
        geometry=[Point(-68 + (i % 17) / 10, -17 + (i % 13) / 10) for i in range(n)],
        crs=4326,
    )


def test_zoom_minimo_segun_extension():
    assert zoom_minimo([-69.7, -22.9, -57.5, -9.7]) == 6
    assert zoom_minimo([-64.0, -17.0, -64.0, -17.0]) == 12
    assert zoom_minimo([-180, -90, 180, 90]) == 1


def test_zoom_maximo_crece_con_los_vertices_y_tiene_tope():
    assert zoom_maximo(100, False, 6) == 8
    assert zoom_maximo(1_000_000, False, 6) == 6 + 3 + 1
    assert zoom_maximo(100, False, 14) == 14
    assert zoom_maximo(100_000, True, 6) > zoom_maximo(100_000, False, 6)


def test_propiedades_descarta_ids_coordenadas_y_constantes(tmp_path):
    ruta = normalizado(tmp_path, puntos())
    propiedades = {p["nombre"]: p for p in elegir_propiedades(ruta, "geometry")}
    assert set(propiedades) == {"departamento", "poblacion", "indice"}
    assert propiedades["departamento"]["categorias"] == ["BENI", "LA PAZ", "ORURO"]
    assert propiedades["poblacion"]["tipo"] == "number" and propiedades["poblacion"]["minimo"] >= 1000


def test_redondea_a_tres_cifras():
    import pyarrow as pa

    salida = redondear_cifras(pa.array([0.123456, 12345.678, 0.0, None], type=pa.float64())).to_pylist()
    assert salida == [0.123, 12300.0, 0.0, None]


def test_construye_pmtiles_con_capa_zoom_y_propiedades(tmp_path):
    destino = tmp_path / "dataset.pmtiles"
    meta = construir_pmtiles(normalizado(tmp_path, puntos()), destino)
    assert meta["min_zoom"] <= meta["max_zoom"] <= 14 and meta["capa"] == "dataset"
    assert [p["nombre"] for p in meta["propiedades"]] == ["departamento", "poblacion", "indice"]
    with open(destino, "rb") as f:
        lector = Reader(MmapSource(f))
        capas = lector.metadata()["vector_layers"]
        assert capas[0]["id"] == "dataset"
        assert set(capas[0]["fields"]) == {"departamento", "poblacion", "indice"}
        assert lector.header()["min_zoom"] == meta["min_zoom"]


def test_pmtiles_acepta_geometrias_mixtas_y_nulas(tmp_path):
    gdf = gpd.GeoDataFrame(
        {"clase": list("abcde")},
        geometry=[
            Point(-65, -17),
            LineString([(-65, -17), (-64, -16)]),
            Polygon([(-66, -18), (-64, -18), (-64, -16)]),
            GeometryCollection([Point(-64, -15), LineString([(-64, -15), (-63, -14)])]),
            None,
        ],
        crs=4326,
    )
    meta = construir_pmtiles(normalizado(tmp_path, gdf), tmp_path / "dataset.pmtiles")
    assert meta["bytes"] > 0


def test_pmtiles_omite_capas_sin_geometrias(tmp_path):
    gdf = gpd.GeoDataFrame({"clase": ["a", "b"]}, geometry=[None, None], crs=4326)
    origen = tmp_path / "vacia.geoparquet"
    gdf.to_parquet(origen, index=False)
    with pytest.raises(PmtilesOmitido):
        construir_pmtiles(origen, tmp_path / "dataset.pmtiles")


def metadata_demo(item="geodatosbolivia_demo_geo_demo_capa", geoserver="demo.geo"):
    return {
        "geoserver": geoserver,
        "fuente": "Servidor de demostración",
        "ows": "https://example.org/geoserver/ows",
        "nombre": "demo:capa",
        "titulo": "Capa de demostración",
        "descripcion": "Puntos de prueba",
        "archive_item": item,
    }


def test_collection_describe_tabla_y_teselas(tmp_path):
    ruta = normalizado(tmp_path, puntos())
    perfil = perfilar(ruta)
    meta = construir_pmtiles(ruta, tmp_path / "dataset.pmtiles")
    c = stac.construir_collection(metadata_demo(), perfil, meta)

    assert c["type"] == "Collection" and c["stac_version"] == "1.1.0" and c["license"] == "other"
    assert c["id"] == "geodatosbolivia_demo_geo_demo_capa"
    assert c["extent"]["spatial"]["bbox"] == [perfil["bbox"]]
    assert c["extent"]["temporal"]["interval"] == [[None, None]]
    assert c["table:row_count"] == 300 and c["table:primary_geometry"] == "geometry"
    nombres = {col["name"]: col["type"] for col in c["table:columns"]}
    assert nombres["departamento"] == "string" and nombres["poblacion"] == "int64" and "bbox" not in nombres
    assert c["summaries"]["vector:geometry_types"] == ["Point"]
    assert c["assets"]["data"]["href"].endswith("/dataset.geoparquet")
    assert c["assets"]["data"]["file:checksum"] == "1220" + perfil["sha256"]
    assert c["assets"]["pmtiles"]["roles"] == ["visual"] and c["assets"]["datapackage"]["href"] == "./datapackage.json"
    assert (c["pmtiles:min_zoom"], c["pmtiles:max_zoom"]) == (meta["min_zoom"], meta["max_zoom"])
    assert c["pmtiles:properties"] == meta["propiedades"]
    enlace = next(link for link in c["links"] if link["rel"] == "pmtiles")
    assert enlace["pmtiles:layers"] == ["dataset"]
    json.dumps(c)


def test_collection_sin_teselas_omite_los_campos_pmtiles(tmp_path):
    ruta = normalizado(tmp_path, puntos())
    c = stac.construir_collection(metadata_demo(), perfilar(ruta), None)
    assert "pmtiles" not in c["assets"] and not any(k.startswith("pmtiles:") for k in c)


def test_catalogos_se_enlazan_entre_si(tmp_path, monkeypatch):
    base = tmp_path / "archivar"
    monkeypatch.setattr(stac, "BASE_DIR", base)
    monkeypatch.setattr(stac, "PUBLICADOS", base / "publicados")
    monkeypatch.setattr(stac, "CATALOGO_RAIZ", base / "catalog.json")

    ruta = normalizado(tmp_path, puntos())
    perfil = perfilar(ruta)
    metadata = metadata_demo()
    carpeta = base / "publicados" / "demo_geo" / "demo_capa"
    stac.escribir(carpeta / "metadata.json", metadata)
    stac.escribir(carpeta / "collection.json", stac.construir_collection(metadata, perfil, None))

    stac.escribir_catalogos()

    def enlaces(path, rel):
        datos = json.loads(path.read_text())
        return [(link["rel"], (path.parent / link["href"]).resolve()) for link in datos["links"] if link["rel"] == rel]

    raiz = base / "catalog.json"
    hijos = [destino for _, destino in enlaces(raiz, "child")]
    assert set(hijos) == {(base / "publicados/demo_geo/catalog.json").resolve()}
    for catalogo in hijos:
        assert enlaces(catalogo, "root")[0][1] == raiz.resolve()
        for _, coleccion in enlaces(catalogo, "child"):
            assert coleccion.exists()
            assert enlaces(coleccion, "parent")[0][1] == catalogo
            assert enlaces(coleccion, "root")[0][1] == raiz.resolve()

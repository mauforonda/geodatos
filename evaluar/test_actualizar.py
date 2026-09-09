import unittest

import pandas as pd

from archivar.actualizar import seleccionar_candidatos
from evaluar.actualizar import construir_base, muestra_tiene_geometrias_validas


def feature(coordinates):
    return {
        "type": "Feature",
        "properties": {},
        "geometry": None if coordinates is None else {"type": "Point", "coordinates": coordinates},
    }


class ValidacionGeometriaTest(unittest.TestCase):
    def test_acepta_coordenadas_wgs84_y_geometrias_nulas_aisladas(self):
        self.assertTrue(
            muestra_tiene_geometrias_validas(
                [feature(None), feature([-63.5773, -17.6923])]
            )
        )

    def test_rechaza_coordenadas_fuera_del_rango_wgs84(self):
        self.assertFalse(
            muestra_tiene_geometrias_validas([feature([438779.25, 8043766.914])])
        )

    def test_rechaza_muestras_mixtas(self):
        self.assertFalse(
            muestra_tiene_geometrias_validas(
                [feature([-63.5773, -17.6923]), feature([438779.25, 8043766.914])]
            )
        )

    def test_rechaza_muestra_sin_geometrias(self):
        self.assertFalse(muestra_tiene_geometrias_validas([feature(None)]))


class SeleccionPendientesTest(unittest.TestCase):
    def test_solo_evalua_datasets_activos_no_archivados_sin_validacion(self):
        columnas = [
            "geoserver", "nombre", "descripcion", "wfs_activo", "fallas_wfs_90d",
            "bbox_area", "n_features", "bytes_estimados", "geometria_valida",
            "errores_evaluar", "errores_archivar", "archivado",
            "fecha_ultima_evaluacion", "fecha_ultimo_archivo_intento", "fecha_archivado",
        ]
        existentes = pd.DataFrame(
            [
                ["g", "pendiente", "", True, 0, 1, 1, 10, pd.NA, 0, 0, False, "2026-01-01", "", ""],
                ["g", "archivado", "", True, 0, 1, 1, 10, pd.NA, 0, 0, True, "2026-01-01", "", ""],
                ["g", "valido", "", True, 0, 1, 1, 10, True, 0, 0, False, "2026-01-01", "", ""],
                ["g", "invalido", "", True, 0, 1, 1, 10, False, 0, 0, False, "2026-01-01", "", ""],
            ],
            columns=columnas,
        )
        existentes["geometria_valida"] = existentes["geometria_valida"].astype("boolean")
        activas = existentes[["geoserver", "nombre", "descripcion", "bbox_area"]].copy()
        fallas = pd.DataFrame({"geoserver": [], "fallas_wfs_90d": []})

        _, pendientes = construir_base(existentes, activas, fallas)

        self.assertEqual(pendientes["nombre"].tolist(), ["pendiente"])


class SeleccionArchivoTest(unittest.TestCase):
    def test_solo_archiva_geometria_validada(self):
        datasets = pd.DataFrame(
            {
                "geoserver": ["g", "g", "g"],
                "nombre": ["valido", "invalido", "pendiente"],
                "wfs_activo": [True, True, True],
                "archivado": [False, False, False],
                "geometria_valida": pd.Series([True, False, pd.NA], dtype="boolean"),
                "fecha_ultima_evaluacion": ["2026-01-01"] * 3,
                "bytes_estimados": [10, 10, 10],
                "errores_archivar": [0, 0, 0],
            }
        )

        candidatos = seleccionar_candidatos(datasets, set(), pd.DataFrame())

        self.assertEqual(candidatos["nombre"].tolist(), ["valido"])


if __name__ == "__main__":
    unittest.main()

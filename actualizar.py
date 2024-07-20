#!/usr/bin/env python3

import json
import xmltodict
import requests
from requests.adapters import HTTPAdapter
import urllib3
import pandas as pd
import datetime as dt
import os
from itertools import chain
import re
from tqdm import tqdm
import pytz

""" 
El objetivo de este programa es mantener un inventario de 
capas en servidores geográficos útiles para estudiar Bolivia. 

El DIRECTORIO lista estos servidores y sus servicios disponibles,
que pueden ser wms (para capas raster) o wfs (para capas vectoriales).
Este programa consulta cada servicio disponible y recolecta una lista
de capas geográficas que consolida en el inventario CAPAS. Si este 
programa corre periódicamente, el inventario debería permitir responder:

- ¿Qué capas están disponibles?
- ¿Qué capas fueron subidas o removidas recientemente?

Junto con el DIRECTORIO, este inventario debería permitir consultar 
capas disponibles, siguiendo las especificaciones wms y wfs.

Además produce un LOG de eventos que debería permitir responder:

- ¿Qué servidores y servicios están disponibles en cada corrida y cuánto
tardan en responder? 
- ¿Qué servidores y servicios presentan errores en cada corrida?
"""


# La lista de servidores y servicios disponibles
DIRECTORIO = "directorio.json"
# El log de eventos
LOG = "log.csv"
# El inventario de todas las capas
CAPAS = "capas.csv"


def iniciarSesion(reintentos=3) -> requests.sessions.Session:
    """
    Crea una sesión con la cual realizar todas las consultas de red
    y deshabilita las advertencias que resultarían de consultas inseguras,
    necesarias para trabajar con servidores sin certificados válidos.
    """
    sesion = requests.Session()
    sesion.mount("http://", HTTPAdapter(max_retries=reintentos))
    sesion.mount("https://", HTTPAdapter(max_retries=reintentos))
    urllib3.disable_warnings()
    return sesion


def consultar_geoserver(geoserver: dict, sesion: requests.sessions.Session):
    """
    Consulta información de todas las capas disponibles en los servicios
    wms y wfs de un geoserver, que adjunta al diccionario capas.
    """

    def consultar_capabilities(
        geoserver: dict, servicio: str, sesion: requests.sessions.Session
    ):
        """
        Consulta el endpoint getCapabilities del servicio wms o wfs en un geoserver.
        """

        # Versiones predefinidas de servicios para tener respuestas con estructuras predecibles
        version = dict(wms="1.3.0", wfs="1.0.0")

        # Realizar la consulta y devolver la respuesta
        return sesion.get(
            f"{geoserver["ows"]}?service={servicio}&version={version[servicio]}&request=GetCapabilities",
            timeout=20,
            verify=False,
        )

    def encontrar_capas(capabilities: requests.Response, servicio: str):
        """
        Encuentra y devuelve una lista de capas en una respuesta de getCapabilities.
        """

        def asegurar_encoding(capabilities: requests.Response):
            """
            Convierte la respuesta de getCapabilities a UTF-8 si parece tener
            otro encoding, usualmente ISO_8859-1.
            """

            if capabilities.encoding and capabilities.encoding != "UTF-8":
                return capabilities.text.encode(capabilities.encoding).decode("UTF-8")
            else:
                return capabilities.text

        # La ubicación de la lista de capas en ambos servicios
        capas_en_capabilities = dict(
            wms=["WMS_Capabilities", "Capability", "Layer", "Layer"],
            wfs=["WFS_Capabilities", "FeatureTypeList", "FeatureType"],
        )

        # Convertir la respuesta a un diccionario en formato UTF-8
        capas = xmltodict.parse(asegurar_encoding(capabilities))

        # Navegar este diccionario hasta encontrar la lista de capas y devolverla
        for nivel in capas_en_capabilities[servicio]:
            capas = capas.setdefault(nivel, {})

        return capas

    def procesar_capa(capas_geoserver: dict, capa: dict, servicio: str):
        """
        Procesa la información de una capa para ser adjuntada al objeto capas_geoserver.
        El objeto capas_geoserver es un diccionario cuyas llaves son nombres de capas
        y cuyos valores son diccionarios de un nivel con atributos de cada capa.

        La mayoría de las capas probablemente estarán disponibles simultáneamente en
        los servicios wms y wfs. Cuando esta función procesa una capa que ya se encuentra
        en el objeto capas_geoserver, lo actualiza para declarar su disponibilidad en
        el servicio correspondiente.

        Esta función intenta ignorar las capas de muestra que se distribuyen en
        instalaciones de geoserver.
        """

        def formatear_capa(capa: dict, servicio: str):
            """
            Procesa la información de una capa para
            devolver un diccionario de un solo nivel.
            """

            def formatear_epsg(epsg):
                """
                Toma el campo de sistema de coordenadas y devuelve un solo
                número entero para el valor EPSG.
                """
                if type(epsg) is list:
                    epsg = [campo for campo in epsg if "EPSG" in campo][0]
                return int(epsg.split(":")[-1])

            def formatear_descripcion(descripcion):
                """
                Intenta normalizar descripciones vacías como valores nulos.
                """
                if descripcion in [
                    "No abstract provided.",
                    "No abstract provided",
                    "REQUIRED: A brief narrative summary of the data set.",
                    "No hay resumen proporcionado",
                ]:
                    return None
                else:
                    return descripcion

            # Campos que tomar de cada capa y qué nombre asignarles
            campos = dict(
                wms=[
                    "Title",
                    "Abstract",
                    "CRS",
                    "EX_GeographicBoundingBox",
                ],
                wfs=[
                    "Title",
                    "Abstract",
                    "SRS",
                    "LatLongBoundingBox",
                ],
                nombres=["titulo", "descripcion", "epsg", "encuadre"],
            )

            # El orden y nombres de campos que representan límites
            encuadre = dict(
                wms=[
                    "westBoundLongitude",
                    "eastBoundLongitude",
                    "southBoundLatitude",
                    "northBoundLatitude",
                ],
                wfs=[
                    "@minx",
                    "@maxx",
                    "@miny",
                    "@maxy",
                ],
                nombres=["min_x", "max_x", "min_y", "max_y"],
            )

            # Inicializar un objeto para la información de la capa
            entrada = {}

            # Para cada campo en la lista de campos seleccionados
            for campo, nombre in zip(campos[servicio], campos["nombres"]):
                # Normalizar descripciones
                if nombre == "descripcion":
                    entrada[nombre] = formatear_descripcion(capa[campo])

                # Formatear sistemas de coordenadas como valores EPSG
                elif nombre == "epsg":
                    entrada[nombre] = formatear_epsg(capa[campo])

                # Desempacar y formatear valores de la caja de límites
                elif nombre == "encuadre":
                    for limite, limite_nombre in zip(
                        encuadre[servicio], encuadre["nombres"]
                    ):
                        entrada[limite_nombre] = float(capa[campo][limite])

                # Copiar el resto de campos
                else:
                    entrada[nombre] = capa[campo]

            # Declarar la disponibilidad del servicio correspondiente
            entrada[servicio] = True

            # Devolver la información formateada de la capa
            return entrada

        def muestra_geoserver(nombre):
            """
            Intenta identificar si el nombre de una capa corresponde a una de las
            capas de muestra que se distribuyen en instalaciones de geoserver.
            """
            return re.findall(
                r"^(topp\:|sf\:|ne\:|tiger\:|nurc\:|spearfish|tasmania|tiger-ny)",
                nombre,
            )

        # Si la capa no es una muestra
        capa_nombre = capa["Name"]
        if not muestra_geoserver(capa_nombre):
            # Si la capa ya está en capas_geoserver,
            # sólo actualizar su disponibilidad en el servicio correspondiente
            if capa_nombre in capas_geoserver.keys():
                capas_geoserver[capa_nombre][servicio] = True

            # Si no está, crear un nuevo objeto con la información bien formateada
            else:
                capas_geoserver[capa_nombre] = formatear_capa(capa, servicio)

        # Devolver el objeto capas_geoserver
        return capas_geoserver

    def consultar_servicio(
        capas_geoserver: dict,
        geoserver: dict,
        servicio: str,
        sesion: requests.sessions.Session,
    ):
        """
        Consulta y procesa información de las capas disponibles en un servicio (wms o wfs)
        de un geoserver. Si encuentra errores, los adjunta a una lista global de errores.
        """

        # Inicializar objetos vacíos donde colocar capas y errores.
        capas_servicio = []
        error = None

        # Una estampa de tiempo para posibles errores
        timestamp = dt.datetime.now(pytz.timezone("America/La_Paz")).isoformat(
            timespec="minutes"
        )

        try:
            # Consultar el endpoint getCapabilities
            capabilities = consultar_capabilities(geoserver, servicio, sesion)

            # Si el servidor responde correctamente
            if capabilities.status_code == 200:
                # Encontrar en la respuesta la lista de capas
                capas_servicio = encontrar_capas(capabilities, servicio)

                # Si esta lista no está vacía, procesar cada capa
                if capas_servicio:
                    for capa in capas_servicio:
                        capas_geoserver = procesar_capa(capas_geoserver, capa, servicio)
            else:
                # Si el servidor no responde correctamente
                error = f"estatus de capabilities: {capabilities.status_code}"

        # Si ocurre un error de red
        except requests.exceptions.RequestException as e:
            error = f"error de red: {str(e)}"

        # Si ocurre cualquier otro tipo de error
        except Exception as e:
            error = str(e)

        # Adjuntar el error al log
        if error:
            sesion_log.append(
                dict(
                    tiempo=timestamp,
                    geoserver=geoserver["nombre"],
                    servicio=servicio,
                    evento="error",
                    descripcion=error,
                )
            )

        # o declarar que todo parece funcionar bien
        else:
            sesion_log.append(
                dict(
                    tiempo=timestamp,
                    geoserver=geoserver["nombre"],
                    servicio=servicio,
                    evento="ok",
                    descripcion=f"{capabilities.elapsed.total_seconds()} segundos",
                )
            )

        # Devolver el objeto con capas del geoserver
        return capas_geoserver

    # Inicializar un objeto vacío donde colocar capas
    capas_geoserver = {}

    # Para cada servicio
    for servicio in ["wms", "wfs"]:
        # Si el servicio está habilitado en el directorio
        if geoserver[servicio]:
            # Consultar las capas disponibles y adjuntarlas al objeto capas_geoserver
            capas_geoserver = consultar_servicio(
                capas_geoserver, geoserver, servicio, sesion
            )

        # Si hay capas, adjuntar el objeto capas_geoserver al objeto global capas
        if capas_geoserver:
            capas[geoserver["nombre"]] = capas_geoserver


def manejar_log(sesion_log: list):
    """
    Consolida eventos en la sesión en un log con eventos históricos,
    identifica y deshabilita servicios que produjeron demasiados errores
    recientemente, y guarda este log.
    """

    def consolidar_log(sesion_log: list):
        """
        Consolida eventos en la sesión con un log histórico.
        """

        log = pd.DataFrame(sesion_log)
        if os.path.exists(LOG):
            historial = pd.read_csv(LOG, parse_dates=["tiempo"])
            log = pd.concat([historial, log]).sort_values(
                ["tiempo", "geoserver", "servicio"]
            )
        log["tiempo"] = pd.to_datetime(log.tiempo)
        return log

    def encontrar_servicios_rotos(log: pd.core.frame.DataFrame, n=10):
        """
        Identifica servicios que fallaron n veces en los últimos n días.
        Asumiendo que el script corre una vez al día, debería identificar
        casos donde un servicio falla consecutivamente por n días.
        """

        log_seleccion = (
            log[
                (log.tiempo.dt.date >= dt.datetime.now().date() - dt.timedelta(days=n))
                & (log.evento == "error")
            ]
            .groupby(["geoserver", "servicio"])
            .size()
            .copy()
        )
        log_seleccion = log_seleccion[log_seleccion >= n]
        return log_seleccion.reset_index()[["geoserver", "servicio"]].to_dict(
            orient="records"
        )

    def deshabilitar_servicios_rotos(
        log: pd.core.frame.DataFrame,
        servicios_rotos: list,
        timestamp: str,
        n=10,
    ):
        """
        Modifica el directorio para deshabilitar servicios y adjunta un evento
        correspondiente en el log.
        """

        gs = pd.DataFrame(geoservers)
        for roto in servicios_rotos:
            gs.loc[gs.nombre == roto["geoserver"], roto["servicio"]] = False
        with open(DIRECTORIO, "w") as f:
            json.dump(gs.to_dict(orient="records"), f, ensure_ascii=False, indent=2)
        rotos_log = pd.DataFrame(
            [
                dict(
                    tiempo=timestamp,
                    geoserver=roto["geoserver"],
                    servicio=roto["servicio"],
                    evento="servicio_deshabilitado",
                    descripcion=f"tras {n} días con errores",
                )
                for roto in servicios_rotos
            ]
        )
        return pd.concat([log, rotos_log])

    # Una estampa de tiempo para eventos en el log
    timestamp = dt.datetime.now(pytz.timezone("America/La_Paz")).isoformat(
        timespec="minutes"
    )

    # Consolido eventos en esta sesión con logs reunidos desde la primera corrida.
    log = consolidar_log(sesion_log)

    # Identificar servicios que han fallado demasiadas veces recientemente y deshabilitarlos
    servicios_rotos = encontrar_servicios_rotos(log)
    if servicios_rotos:
        log = deshabilitar_servicios_rotos(log, servicios_rotos, timestamp)

    # Guardar el log
    log["tiempo"] = pd.to_datetime(log.tiempo)
    log.sort_values(["tiempo", "geoserver", "servicio"], na_position="first").to_csv(
        LOG, index=False, date_format="%Y-%m-%d %H:%M %z"
    )


def manejar_capas(capas: dict):
    """
    Construye un dataframe con información de las capas disponibles
    en geoservers consultados y actualiza el csv CAPAS, que registra
    información de todas las capas vistas desde la primera corrida.
    """

    def construir_tabla_disponibles(capas):
        """
        Construir una tabla en la forma de CAPAS.
        """

        # Aplanar el objeto capas en una lista de diccionarios y crear un dataframe
        disponibles = pd.DataFrame(
            list(
                chain.from_iterable(
                    [
                        [
                            {
                                **{"geoserver": geoserver, "nombre": capa},
                                **capas[geoserver][capa],
                            }
                            for capa in capas[geoserver].keys()
                        ]
                        for geoserver in capas.keys()
                    ]
                )
            )
        )

        # Arreglar campos donde un servicio no está disponible
        for servicio in ["wms", "wfs"]:
            disponibles.loc[disponibles[servicio].isna(), servicio] = False

        # Incluir columnas de fechas
        for columna, valor in zip(["fecha_encontrado", "fecha_removido"], [None, None]):
            disponibles[columna] = valor

        # Devolver tabla
        return disponibles

    def actualizar_historial(disponibles: pd.core.frame.DataFrame):
        """
        Utiliza la tabla de capas disponibles para actualizar los registros históricos.
        """

        def clasificar_capas(
            disponibles: pd.core.frame.DataFrame,
            historial: pd.core.frame.DataFrame,
            columnas_indice: list,
        ):
            """
            Compara las columnas_indice entre las tablas de capas disponibles e históricas
            para identificar cuáles capas aparecen por primera vez, cuáles se repiten y cuáles
            sólo figuran en el registro histórico. Devuelve una lista para cada caso con los
            valores en las columnas_indice.
            """

            # Compara ambas tablas y clasifica sus capas
            clasificados = pd.merge(
                historial[columnas_indice],
                disponibles[columnas_indice],
                how="outer",
                indicator=True,
            )

            # Asigna nombres más legibles a estas categorías
            clasificados["_merge"] = clasificados._merge.cat.rename_categories(
                dict(left_only="faltante", right_only="nuevo", both="igual")
            )

            # Devuelve una lista para cada categoría
            return [
                clasificados[clasificados._merge == clase][
                    columnas_indice
                ].values.tolist()
                for clase in ["nuevo", "igual", "faltante"]
            ]

        def filtrar_tabla(
            tabla: pd.core.frame.DataFrame, filtro: list, columnas_indice: list
        ):
            return tabla[tabla.set_index(columnas_indice).index.isin(filtro)].copy()

        def preparar_capas_nuevas(
            historial: pd.core.frame.DataFrame,
            disponibles: pd.core.frame.DataFrame,
            nuevas: list,
            columnas_indice: list,
            timestamp: str,
        ):
            """
            Prepara 2 tablas de capas nuevas.
            - Capas en servidores que consulto por primera vez, donde no puedo
            especificar el valor de fecha_encontrado.
            - Capas en servidores que conozco, donde asigno la fecha de hoy
            como fecha_encontrado
            """

            # Las capas nuevas en la tabla de capas disponibles
            capas_nuevas = filtrar_tabla(disponibles, nuevas, columnas_indice)

            # Cuáles de estas capas vienen de servidores que parece que
            # consulto por primera vez
            geoservers_conocidos = historial.geoserver.unique()
            capas_en_nuevos_geoservers = capas_nuevas[
                ~capas_nuevas.geoserver.isin(geoservers_conocidos)
            ].copy()

            # Cuáles vienen de servidores conocidos
            capas_nuevas = capas_nuevas[
                capas_nuevas.geoserver.isin(geoservers_conocidos)
            ]

            # Declarar que esta capas fueron encontradas hoy
            capas_nuevas.loc[:, "fecha_encontrado"] = timestamp

            # Devolver ambas tablas
            return capas_en_nuevos_geoservers, capas_nuevas

        def preparar_capas_iguales(
            historial: pd.core.frame.DataFrame,
            disponibles: pd.core.frame.DataFrame,
            iguales: list,
            columnas_indice: list,
        ):
            """
            Prepara una tabla de capas que se repiten.
            Mantiene todas las columnas en la tabla disponibles y
            copia fecha_encontrado de la tabla histórica.
            """

            # 2 tablas con las capas repetidas en la tabla disponibles
            # y la tabla historial, ordenadas de manera que pueda mover
            # fácilmente columnas entre ellas
            historial_iguales, disponibles_iguales = [
                filtrar_tabla(tabla, iguales, columnas_indice)
                .sort_values(columnas_indice)
                .reset_index(drop=True)
                for tabla in [historial, disponibles]
            ]

            # Copiar la columna fecha_encontrado de historial a disponibles
            disponibles_iguales.loc[:, "fecha_encontrado"] = (
                historial_iguales.fecha_encontrado
            )

            # Devolver la nueva tabla de disponibles
            return disponibles_iguales

        def preparar_capas_faltantes(
            historial: pd.core.frame.DataFrame,
            faltantes: list,
            columnas_indice: list,
            timestamp: str,
        ):
            """
            Asumo 3 escenarios:
            1. Capas que faltan porque sus servidores están inaccesibles.
            2. Capas que faltan porque sus servidores desaparecen del directorio.
            3. Capas que faltan porque fueron removidas de su servidor.
            En los 3 escenarios, ambos servicios (wms, wfs) no están disponibles.
            Y sólo en el escenario 3 es correcto declarar que una capa ha sido removida
            asignando un valor a la columna fecha_removido.
            """

            # Capas en el registro histórico que no están disponibles
            historial_faltantes = filtrar_tabla(historial, faltantes, columnas_indice)
            for servicio in ["wms", "wfs"]:
                historial_faltantes.loc[:, servicio] = False

            # Capas en esta tabla cuyos servidores están accesibles y siguen en el directorio
            historial_faltantes.loc[
                (
                    ~historial_faltantes.geoserver.isin(
                        [i["geoserver"] for i in sesion_log if i["evento"] == "error"]
                    )
                )
                & (
                    historial_faltantes.geoserver.isin(
                        [g["nombre"] for g in geoservers]
                    )
                ),
                "fecha_removido",
            ] = timestamp

            return historial_faltantes

        # Si el csv de registros históricos existe
        if os.path.exists(CAPAS):
            # Una estampa de tiempo para actualizar valores en las columnas de fecha
            timestamp = pd.to_datetime(dt.datetime.now(pytz.timezone("America/La_Paz")))

            # Las columnas que comparamos para determinar si una capa disponible
            # está presente en el registro histórico
            columnas_indice = ["geoserver", "nombre"]

            # Leer el registro histórico
            historial = pd.read_csv(
                CAPAS, parse_dates=["fecha_encontrado", "fecha_removido"]
            )

            # Para incorporar la información de capas disponibles en el registro histórico
            # considero 3 tipos de capas: aquellas que aparecen por primera vez, aquellas
            # que se aparecen nuevamente y las que dejan de aparecer.

            nuevas, iguales, faltantes = clasificar_capas(
                disponibles, historial, columnas_indice
            )

            # Existe al menos una forma de incoporar cada tipo de capa en el registro histórico.

            # Para las capas nuevas, primero separo las que aparecen porque se agrega un nuevo
            # geoserver. Me interesa que el campo fecha_encontrado refleje cuándo aproximadamente
            # aparece una nueva capa en un servidor. Sería incorrecto decir que una capa aparece
            # cuando consulto el servidor por primera vez. Así que en estos casos (capas_en_nuevos_geoservers),
            # dejo el campo fecha_encontrado en blanco.

            # El resto de capas nuevas aparecen en servidores que ya conozco. Éstas son capas que
            # fueron probablemente subidas al servidor recientemente y son particularmente
            # interesantes. En ellas asigno el día de hoy como el valor de fecha_encontrado.

            capas_en_nuevos_geoservers, capas_nuevas = preparar_capas_nuevas(
                historial, disponibles, nuevas, columnas_indice, timestamp
            )

            # Para capas que aparecen nuevamente, creo una tabla que incluye las columnas
            # de disponibilidad (wms, wfs) en la tabla disponibles, y la columa de creación
            # (fecha_encontrado) en la tabla histórica. Para el resto de campos, asumo que
            # la tabla más nueva (disponibles) es más correcta.

            capas_iguales = preparar_capas_iguales(
                historial, disponibles, iguales, columnas_indice
            )

            # Para capas que desaparecen, me interesan en particular aquellas que parecen haber sido
            # removidas de un servidor, y las distingo de capas en servidores inaccesibles o que desaparecen
            # del directorio. Sólo en capas removidas asigno un valor a la columna fecha_removido. En todos
            # estos casos declaro ambos servicios (wms, wfs) no disponibles.

            capas_faltantes = preparar_capas_faltantes(
                historial, faltantes, columnas_indice, timestamp
            )

            # Concatenar todas estas tablas en una sola
            nuevo_historial = pd.concat(
                [
                    capas_faltantes,
                    capas_iguales,
                    capas_nuevas,
                    capas_en_nuevos_geoservers,
                ]
            )

            # Asegurarme que las fechas sean legibles
            for col in ["fecha_encontrado", "fecha_removido"]:
                nuevo_historial[col] = pd.to_datetime(nuevo_historial[col])

        # Si ésta es la primera vez que corro el script
        else:
            nuevo_historial = disponibles

        # Guardar la nueva tabla histórica con un orden predecible para producir
        # diffs significativos, y con formatos de decimales y fecha predecibles
        nuevo_historial.sort_values(
            ["fecha_encontrado", "geoserver", "nombre"], na_position="first"
        ).to_csv(CAPAS, float_format="%.6f", date_format="%Y-%m-%d", index=False)

    # Construir una tabla en la forma de CAPAS
    disponibles = construir_tabla_disponibles(capas)
    # Utilizar esta tabla para actualizar registros históricos
    actualizar_historial(disponibles)


if __name__ == "__main__":
    capas, sesion_log = [{}, []]
    sesion = iniciarSesion()
    geoservers = json.load(open(DIRECTORIO, "r"))
    for geoserver in tqdm(geoservers, total=len(geoservers)):
        consultar_geoserver(geoserver, sesion)
    if capas:
        manejar_capas(capas)
    if sesion_log:
        manejar_log(sesion_log)

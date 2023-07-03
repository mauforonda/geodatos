#!/usr/bin/env python3

import xmltodict
import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import json
from datetime import datetime as dt
import os

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
GEOSERVER_DIRECTORY = "update/geoservers.json"
CSV_OUTPUT = "datos.csv"
CSV_VISTO = "visto.csv"
CSV_RECIENTE = "reciente.csv"
MAX_RETRIES = 10
TIMEOUT = 20


def start_session() -> requests.sessions.Session:
    session = requests.Session()
    session.mount("http://", HTTPAdapter(max_retries=MAX_RETRIES))
    session.mount("https://", HTTPAdapter(max_retries=MAX_RETRIES))
    return session


def index() -> None:
    # fields to index
    fields = {"Name": "nombre", "Title": "titulo", "Abstract": "descripcion"}

    # service-specific parameters
    services = {
        "wms": {
            "url": "?service=wms&request=GetCapabilities",
            "layers": ["WMS_Capabilities", "Capability", "Layer", "Layer"],
        },
        "wfs": {
            "url": "?service=wfs&version=1.0.0&request=GetCapabilities",
            "layers": ["WFS_Capabilities", "FeatureTypeList", "FeatureType"],
        },
    }

    def index_geoserver(entry: dict) -> pd.core.frame.DataFrame:
        try:
            # fetch the data
            service = services[entry["service"]]
            url = f'{entry["ows"]}{service["url"]}'
            response = session.get(url, timeout=TIMEOUT, verify=False)

            # select the list of layers
            layers = xmltodict.parse(response.text)
            for level in service["layers"]:
                layers = layers.setdefault(level, {})
            if layers:
                # make a dataframe
                df = pd.json_normalize(layers)
                df = df[fields.keys()]
                df = df.rename(columns=fields)

                # force unicode if necessary
                if entry["decode"]:
                    for column in df.columns:
                        df[column] = df[column].apply(
                            lambda _: _.encode("iso_8859-1").decode("utf-8")
                            if type(_) == str
                            else _
                        )
                df.insert(0, "sistema", entry["short_name"])
                print(f'{entry["ows"]} -> {df.shape[0]} capas')
                return df
            else:
                print(f'{entry["ows"]} -> 0 capas')
                return pd.DataFrame()
        except Exception as e:
            print(f'{entry["ows"]} -> {e}')
            return pd.DataFrame()

    with open(GEOSERVER_DIRECTORY, "r") as f:
        geoservers = json.load(f)

    # find all available layers
    dfs = []
    for gs in geoservers:
        df = index_geoserver(gs)
        dfs.append(df)
    output = pd.concat(dfs)
    output.sort_values(["sistema", "nombre"]).to_csv(CSV_OUTPUT, index=False)

    # find new layers
    basic_columns = ["sistema", "nombre"]
    new_sources = []
    if os.path.exists(CSV_VISTO):
        visto = pd.read_csv(CSV_VISTO)
        merged = output[basic_columns].merge(
            visto, on=basic_columns, how="left", indicator=True
        )
        nuevo = merged[merged._merge == "left_only"][basic_columns]

        # maintain a list of all seen layers
        pd.concat([visto, nuevo]).sort_values(basic_columns).to_csv(
            CSV_VISTO, index=False
        )
        # did I update the source list
        new_sources = [s for s in output.sistema.unique() if s not in visto.sistema.unique()]
    else:
        nuevo = output[basic_columns]
        nuevo.sort_values(basic_columns).to_csv(CSV_VISTO, index=False)
    # save a list of recently added layers, except from just added sources
    nuevo = nuevo[~nuevo.sistema.isin(new_sources)]
    print(f'Nuevas capas: {nuevo.shape[0]}')
    if nuevo.shape[0] > 0:
        nuevo["encontrado"] = dt.now().strftime("%Y-%m-%d")
        if os.path.exists(CSV_RECIENTE):
            reciente = pd.read_csv(CSV_RECIENTE)
            pd.concat([reciente, nuevo]).sort_values("encontrado", ascending=False).to_csv(
                CSV_RECIENTE, index=False
            )
        else:
            nuevo.to_csv(CSV_RECIENTE, index=False)


session = start_session()
index()

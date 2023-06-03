#!/usr/bin/env python3

import xmltodict
import requests
from requests.adapters import HTTPAdapter
from urllib3.exceptions import InsecureRequestWarning
import pandas as pd
import json

requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)
GEOSERVER_DIRECTORY = "update/geoservers.json"
CSV_OUTPUT = "datos.csv"
MAX_RETRIES = 20
TIMEOUT = 30


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
            "url": "geoserver/ows?service=wms&request=GetCapabilities",
            "layers": ["WMS_Capabilities", "Capability", "Layer", "Layer"],
        },
        "wfs": {
            "url": "geoserver/ows?service=wfs&version=1.0.0&request=GetCapabilities",
            "layers": ["WFS_Capabilities", "FeatureTypeList", "FeatureType"],
        },
    }

    def index_geoserver(entry: dict) -> pd.core.frame.DataFrame:
        try:
            # fetch the data
            service = services[entry["service"]]
            url = f'{entry["domain"]}/{service["url"]}'
            if entry["encodeuri"]:
                url = url.replace("&", "%26")
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
                print(f'{entry["domain"]} -> {df.shape[0]} capas')
                return df
            else:
                print(f'{entry["domain"]} -> 0 capas')
                return pd.DataFrame()
        except Exception as e:
            print(f'{entry["domain"]} -> {e}')
            return pd.DataFrame()

    with open(GEOSERVER_DIRECTORY, "r") as f:
        geoservers = json.load(f)

    dfs = []
    for gs in geoservers:
        df = index_geoserver(gs)
        dfs.append(df)
        output = pd.concat(dfs)
        output.sort_values(["sistema", "nombre"]).to_csv(CSV_OUTPUT, index=False)
    return pd.concat(dfs)


session = start_session()
index()

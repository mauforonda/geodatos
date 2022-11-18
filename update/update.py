#!/usr/bin/env python3

import xmltodict
import requests
from requests.adapters import HTTPAdapter
import pandas as pd
import json

GEOSERVER_DIRECTORY = 'update/geoservers.json'
MAX_RETRIES = 20
TIMEOUT = 30

def load_geoservers():
    '''
    Loads the geoserver directory at GEOSERVER_DIRECTORY
    '''
    
    with open(GEOSERVER_DIRECTORY, 'r') as f:
        return json.load(f)

def create_session():
    '''
    Creates an http session to make requests
    '''
    
    session = requests.Session()
    session.mount('http://', HTTPAdapter(max_retries=MAX_RETRIES))
    session.mount('https://', HTTPAdapter(max_retries=MAX_RETRIES))
    return session

def fix_microsoft(layers):
    '''
    Fixes field values formatted in microsoft environments
    '''

    for column in ['nombre', 'titulo', 'abstract']:
        layers[column] = layers[column].apply(lambda x: x.encode("iso-8859-1").decode('utf-8') if type(x) == str else x)

    layers['keywords'] = layers['keywords'].apply(lambda keys: [k.encode("iso-8859-1").decode('utf-8') for k in keys])
    return layers

def download_link(domain, nombre, file_format, encodeuri=False):
    '''
    Formats a download link for file_format
    '''

    url = 'http://{}/geoserver/ows?service=WFS&request=GetFeature&typeName={}&outputFormat={}'.format(
        domain,
        nombre,
        file_format)

    if encodeuri:
        url = url.replace('&', '%26')
        
    return url

def make_download_links(serie, domain, file_format, encodeuri):
    '''
    Returns a series of download links for file_format
    '''

    return serie.apply(lambda layer: download_link(domain, layer, 'application/json', encodeuri))

def format_keywords(serie):
    '''
    Formats keywords
    '''

    return serie.fillna('').apply(
        lambda keys: [k for k in keys if type(k) == str] if type(keys) == list else keys
    )

def make_url(service, domain, encodeuri):
    '''
    Returns the capabilities url for a geoserver domain and service
    '''
    
    if service == 'wms':
        url = 'http://{}/geoserver/ows?service=wms&request=GetCapabilities'.format(domain)
    elif service == 'wfs':
        url = 'http://{}/geoserver/ows?service=wfs&version=1.0.0&request=GetCapabilities'.format(domain)

    if encodeuri:
        url = url.replace('&', '%26')

    return url

def get_capabilities(url):
    '''
    Downloads a geoserver capabilities and returns a dict
    '''

    response = session.get(url, timeout=TIMEOUT)
    capabilities = xmltodict.parse(response.text)
    return capabilities

def parse_capabilites(service, capabilities):
    '''
    Returns a dataframe of layers from capabilities
    '''

    fields = {
        'wms': {
            'Name': 'nombre',
            'Title': 'titulo',
            'Abstract': 'abstract',
            'CRS': 'crs',
            'EX_GeographicBoundingBox.westBoundLongitude': 'limite_oeste',
            'EX_GeographicBoundingBox.eastBoundLongitude': 'limite_este',
            'EX_GeographicBoundingBox.southBoundLatitude': 'limite_sur',
            'EX_GeographicBoundingBox.northBoundLatitude': 'limite_norte',
            'KeywordList.Keyword': 'keywords'
        },
        'wfs': {
            'Name': 'nombre',
            'Title': 'titulo',
            'Abstract': 'abstract',
            'SRS': 'crs',
            'LatLongBoundingBox.@minx': 'limite_oeste',
            'LatLongBoundingBox.@maxx': 'limite_este',
            'LatLongBoundingBox.@miny': 'limite_sur',
            'LatLongBoundingBox.@maxy': 'limite_norte',
            'Keywords': 'keywords'
        }
    }
    if service == 'wms':
        layers = capabilities['WMS_Capabilities']['Capability']['Layer']['Layer']
    elif service == 'wfs':
        layers = capabilities['WFS_Capabilities']['FeatureTypeList']['FeatureType']

    layers = pd.json_normalize(layers)
    layers = layers[fields[service].keys()]
    layers = layers.rename(columns=fields[service])
    return layers

def format_layers(layers, name, domain, decode, encodeuri):
    '''
    Additional changes to the layers dataframe
    '''

    layers['geojson'] = make_download_links(layers.nombre, domain, 'application/json', encodeuri)
    layers['csv'] = make_download_links(layers.nombre, domain, 'csv', encodeuri)
    layers['keywords'] = format_keywords(layers.keywords)
    layers.insert(0, 'sistema', name)
    if decode:
        layers = fix_microsoft(layers)
    return layers

def scan(domain, name, service, decode=True, encodeuri=False):
    '''
    Downloads and formats data from a geoserver domain
    '''

    try:
        url = make_url(service, domain, encodeuri)
        capabilities = get_capabilities(url)
        layers = parse_capabilites(service, capabilities)
        layers = format_layers(layers, name, domain, decode, encodeuri)
        return layers
    except Exception as e:
        print(e)
        return pd.DataFrame()
    
def scan_geoservers(geoservers):
    '''
    Downloads, formats and consolidates data from all geoservers
    '''

    geodata = []
    for geoserver in geoservers:
        print(geoserver['domain'])

        layers = scan(
            domain = geoserver['domain'],
            name = geoserver['short_name'],
            service = geoserver['service'],
            decode = geoserver['decode'],
            encodeuri = geoserver['encodeuri'])
        
        geodata.append(layers)
    return pd.concat(geodata)

def save(geodata):
    '''
    Saves data
    '''

    geodata = geodata.sort_values(['sistema', 'nombre'])
    geodata.to_csv('geodatos.csv', index=False)

    small = geodata[['sistema', 'nombre', 'titulo', 'abstract']]
    small.to_csv('geodatos_small.csv', index=False)

# Load the geoserver directory and prepare to download
geoservers = load_geoservers()
session = create_session()

# Download, format, consolidate and save data from all geoservers
geodata = scan_geoservers(geoservers)
save(geodata)

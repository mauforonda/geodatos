Módulo de archivado.

Responsabilidades:

- seleccionar datasets candidatos desde `evaluar/datasets.csv`
- descargar el dataset vectorial desde WFS
- generar un paquete de preservación para Archive.org
- generar un paquete liviano de publicación en `archivar/publicados/`
- mantener `archivar/paquetes.csv` y `archivar/runs.csv`
- actualizar en `evaluar/datasets.csv` sólo columnas de estado de archivado

La ruta pública derivada de cada dataset es determinística:

- `archivar/publicados/<slug_geoserver>/<slug_nombre>/metadata.json`
- `archivar/publicados/<slug_geoserver>/<slug_nombre>/legend.png`
- `archivar/publicados/<slug_geoserver>/<slug_nombre>/map.png`
- `archivar/publicados/<slug_geoserver>/<slug_nombre>/sample.json`
- `archivar/publicados/<slug_geoserver>/<slug_nombre>/datapackage.json`
- `archivar/publicados/<slug_geoserver>/<slug_nombre>/collection.json`

Fuentes excluidas del archivado se declaran en `archivar/blacklist.json`.
Actualmente el filtro opera a nivel de `geoserver`.

## GeoParquet

Cada `dataset.geoparquet` está pensado para consultas por extensión con DuckDB: GeoParquet 1.1, EPSG:4326, columna `bbox`, filas en orden Hilbert, hasta 49.152 filas por grupo y compresión ZSTD.
Las geometrías se conservan como las entrega la fuente. [normalizar.py](normalizar.py) corrige geoparquets ya archivados y los marca como `normalizado` en [paquetes.csv](paquetes.csv). Esta tabla también guarda una columna `hash`, un hash del contenido de cada dataset archivado (esquema y filas), útil para identificar datasets duplicados.

Si las coordenadas no corresponden al sistema que declara la fuente (por ejemplo, metros declarados como EPSG:4326), se prueban los sistemas UTM 19S, 20S y 21S y Web Mercator. Se acepta un sistema solo si todas las coordenadas caen dentro de Bolivia y si en ningún otro sistema ocurre eso para la mitad o más de las coordenadas. Si eso ocurre en varios candidatos, se desempata considerando otras capas del mismo geoserver en `capas.csv`.

Los datasets que no se pueden normalizar se anotan en `normalizar_omitidos.csv` y no se reintentan. Estas omisiones pueden ser:

- `crs`: las coordenadas no corresponden al sistema que declara la fuente y no se pudo inferir uno
- `geometria`: la conversión falla por las geometrías
- `pmtiles`: el dataset queda normalizado pero no se puede teselar

## Data Package

Cada dataset publica un descriptor [Data Package v2](https://datapackage.org) con solo propiedades del estándar. Describe la capa, su fuente y el archivo, con el tipo de cada columna, restricciones deducidas de los datos y las categorías de las columnas de texto con pocos valores.

## PMTiles

Cada dataset normalizado con geometrías tiene un `dataset.pmtiles` en su item de Archive.org, construido con [tylertoo](https://geoparquet-io.github.io/tylertoo/) desde el geoparquet. [paquetes.csv](paquetes.csv) guarda su tamaño en `bytes_pmtiles`.

Niveles de zoom:

- El primero es el zoom en que la extensión completa cabe en un mapa de 800 px.
- El último conserva la geometría completa, hasta el zoom 14 del visor.
- Hay dos niveles sobre el primero hasta 200.000 vértices (80.000 en puntos), y cada factor de 4 suma uno.
- Tras construir, se agrega un nivel si una tesela comprimida pasa de 400 KB y se quita uno si el archivo pasa de 60 MB.

Geometría:

- Los polígonos pequeños se reducen a cuadrados en vez de desaparecer.
- Los polígonos no se agujerean al simplificar.

Propiedades:

- Las teselas incluyen columnas numéricas con al menos 2 valores distintos y columnas de texto con 2 a 9 categorías.
- Se excluyen identificadores y coordenadas.
- El máximo es 8 propiedades, primero las de texto.
- Los decimales se redondean a 3 cifras significativas.
- En archivos de más de 1 MB, si las propiedades pesan más del doble que la geometría sola, se descartan primero las numéricas con más de 256 valores y luego todas las numéricas.

Se anotan como `pmtiles` en `normalizar_omitidos.csv` los datasets sin geometrías, con un archivo de más de 150 MB o cuyo teselado falla.

## Catálogo STAC

`archivar/catalog.json` es el catálogo raíz. Cada fuente tiene un `catalog.json` y cada dataset una `collection.json` en `archivar/publicados/<slug_geoserver>/`. Los enlaces son relativos y no hay items.

`actualizar.py` y `normalizar.py` escriben la colección de un dataset cada vez que lo perfilan. `python -m archivar.stac` reconstruye los catálogos desde las colecciones existentes.

Cada colección contiene:

- las columnas con su tipo, el número de filas, los tipos de geometría y la extensión, con las extensiones `table`, `vector`, `projection` y `file`
- enlaces al geoparquet, al `datapackage.json` y al PMTiles
- para el PMTiles, `pmtiles:min_zoom`, `pmtiles:max_zoom`, `pmtiles:tile_type`, `pmtiles:center` y `pmtiles:properties`, que lista cada propiedad de las teselas con sus categorías o su rango
- licencia `other` e intervalo temporal abierto, porque las fuentes no informan ninguno de los dos

Las capas de GeoBolivia 2021 quedan fuera hasta tener geoparquet y PMTiles.

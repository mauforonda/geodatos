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

Fuentes excluidas del archivado se declaran en `archivar/blacklist.json`.
Actualmente el filtro opera a nivel de `geoserver`.

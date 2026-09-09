Módulo de evaluación.

Mantiene `datasets.csv`, un índice versionado de datasets vectoriales candidatos a archivado.

La primera corrida incorpora todas las capas actualmente accesibles vía WFS desde `descubrir/capas.csv`.
Las corridas posteriores actualizan señales locales para todas las filas y sólo realizan evaluación de red para datasets activos, no archivados y con `geometria_valida` pendiente. La muestra WFS se solicita en EPSG:4326 y se reutiliza para estimar el tamaño y validar que las coordenadas estén dentro del rango mundial.

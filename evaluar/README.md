Módulo de evaluación.

Mantiene `datasets.csv`, un índice versionado de datasets vectoriales candidatos a archivado.

La primera corrida incorpora todas las capas actualmente accesibles vía WFS desde `descubrir/capas.csv`.
Las corridas posteriores actualizan señales locales para todas las filas y sólo realizan evaluación de red para capas nuevas.

#!/usr/bin/env python

import pandas as pd
df = pd.read_csv('geodatos.csv')
df[['sistema', 'nombre', 'titulo', 'abstract']].to_csv('docs/mini.csv', index=False)

"""
Ingestión de datos - Reporte de clusteres
-----------------------------------------------------------------------------------------

Construya un dataframe de Pandas a partir del archivo 'clusters_report.txt', teniendo en
cuenta que los nombres de las columnas deben ser en minusculas, reemplazando los espacios
por guiones bajos; y que las palabras clave deben estar separadas por coma y con un solo 
espacio entre palabra y palabra.
"""

import pandas as pd
import numpy as np

def ingest_data():

    columns_names = [
        'cluster',
        'cantidad_de_palabras_clave',
        'porcentaje_de_palabras_clave',
        'principales_palabras_clave'
    ]
    columns_widths = [9, 16, 16, 77]
    
    df = pd.read_fwf('clusters_report.txt', widths = columns_widths, names = columns_names, skiprows = 4)

    df = df.ffill()

    df = df.groupby(['cluster', 'cantidad_de_palabras_clave', 'porcentaje_de_palabras_clave']).agg(' '.join).reset_index()

    df['cluster'] = df['cluster'].astype(np.int64)

    df['cantidad_de_palabras_clave'] = df['cantidad_de_palabras_clave'].astype(np.int64)

    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace('%', '')
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].str.replace(',', '.')
    df['porcentaje_de_palabras_clave'] = df['porcentaje_de_palabras_clave'].astype(np.float64)

    df['principales_palabras_clave'] = df['principales_palabras_clave'].apply(lambda s: ' '.join(s.split()))
    df['principales_palabras_clave'] = df['principales_palabras_clave'].str.replace('.', '')

    return df
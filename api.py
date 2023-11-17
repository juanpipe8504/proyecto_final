import pandas as pd
import requests
import json

def read_df_unido():
    df_unido = pd.read_csv("./api_dag/datos_unidos (1).csv", encoding='latin1')
    return df_unido.to_json(orient='records')

#def read_API():
 #   api_key = '25cb5545c16d4f98a0578df496228c74'
  #  url = f'https://api.openweathermap.org/data/2.5/weather?id=524901&appid={api_key}'
   # data = requests.get(url)

    #if data.status_code == 200:
     #   data = data.json()
      #  return data.get('data', [])
   # else:
    #    print("Error: No se pudo obtener la data de la API")
   #    return []


def transform_API(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='transform_api')
    if str_data is not None:
        json_data = json.loads(str_data)
    else:
        print("Error: No se obtuvo datos válidos de XCom.")
        return
    
    df_unido =pd.json_normalize(data=json_data)

    # df_unido = pd.read_csv("datos_unidos.csv", encoding='latin1')

    df_unido['Temperatura (K)'] = pd.to_numeric(df_unido['Temperatura (K)'], errors='coerce')
    df_unido['Humedad'] = pd.to_numeric(df_unido['Humedad'], errors='coerce')

    nuevo_nombre_columnas = {'DescripciÃ³n del Clima': 'Descripción clima'}
    df_unido.rename(columns=nuevo_nombre_columnas, inplace=True)

    nuevo_nombre_columnas = {'Temperatura (K)': 'Temperatura (C)'}
    df_unido.rename(columns=nuevo_nombre_columnas, inplace=True)

    df_unido['Temperatura (C)'] = df_unido['Temperatura (C)'] - 273.15

    df_unido = df_unido.drop_duplicates()

    columna = 'Descripción clima'
    variable_a_eliminar = 'No disponible'
    df_unido = df_unido[df_unido[columna] != variable_a_eliminar]

    df_unido.fillna(0, inplace=True)
    df_unido.dropna(inplace=True)

    return df_unido.to_json(orient='records')
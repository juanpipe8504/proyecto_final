import pandas as pd
import json
import logging
import pandas as pd
import requests


def read_csv():
    df=pd.read_csv("./api_dag/Traffic_Crashes_-_Crashes (1).csv", encoding="latin1")
    return df.to_json(orient='records')
def transform_datos(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='transform_carros')
    if str_data is not None:
        json_data = json.loads(str_data)
    else:
        print("Error: No se obtuvo datos válidos de XCom.")
        return
    df =pd.json_normalize(data=json_data)

    columns_to_drop = ['LIGHTING_CONDITION', 'FIRST_CRASH_TYPE', 'TRAFFICWAY_TYPE', 'LANE_CNT',
                       'ALIGNMENT', 'ROADWAY_SURFACE_COND', 'ROAD_DEFECT', 'REPORT_TYPE',
                       'INTERSECTION_RELATED_I', 'NOT_RIGHT_OF_WAY_I', 'HIT_AND_RUN_I',
                       'DATE_POLICE_NOTIFIED', 'PRIM_CONTRIBUTORY_CAUSE', 'SEC_CONTRIBUTORY_CAUSE',
                       'BEAT_OF_OCCURRENCE', 'PHOTOS_TAKEN_I', 'STATEMENTS_TAKEN_I', 'DOORING_I',
                       'WORK_ZONE_I', 'WORK_ZONE_TYPE', 'WORKERS_PRESENT_I', 'NUM_UNITS',
                       'CRASH_RECORD_ID', 'RD_NO', 'CRASH_DATE_EST_I', 'TRAFFIC_CONTROL_DEVICE',
                       'DEVICE_CONDITION', 'DAMAGE', 'STREET_DIRECTION', 'MOST_SEVERE_INJURY']
    
    df = df.drop(columns=columns_to_drop)

    columnas_enteros = ['INJURIES_TOTAL', 'INJURIES_FATAL', 'INJURIES_INCAPACITATING',
                        'INJURIES_NON_INCAPACITATING', 'INJURIES_REPORTED_NOT_EVIDENT',
                        'INJURIES_NO_INDICATION', 'INJURIES_UNKNOWN', 'CRASH_HOUR',
                        'CRASH_DAY_OF_WEEK', 'STREET_NO', 'CRASH_MONTH']


    df[columnas_enteros] = df[columnas_enteros].fillna(0).astype(int)

    df['CRASH_TYPE'] = df['CRASH_TYPE'].str.replace('AND', '').str.split('/').str[0].str.strip()

    counted_crash_types = df['CRASH_TYPE'].value_counts()

    dias_semana = {1: 'Lunes',
                   2: 'Martes',
                   3: 'Miércoles',
                   4: 'Jueves',
                   5: 'Viernes',
                   6: 'Sábado',
                   7: 'Domingo'}

    df['CRASH_DAY_OF_WEEK'] = df['CRASH_DAY_OF_WEEK'].map(dias_semana)


     meses = {1: 'Enero',
             2: 'Febrero',
             3: 'Marzo',
             4: 'Abril',
             5: 'Mayo',
             6: 'Junio',
             7: 'Julio',
             8: 'Agosto',
             9: 'Septiembre',
             10: 'Octubre',
             11: 'Noviembre',
             12: 'Diciembre'}

    df['CRASH_MONTH'] = df['CRASH_MONTH'].map(meses)

    df['LOCATION'] = df['LOCATION'].str.replace(r'^POINT \((-?\d+\.\d+) (-?\d+\.\d+)\)', r'\2, \1')
    df.dropna(inplace=True)
    return df.to_json(orient='records')
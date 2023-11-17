from carros import transform_datos
import pandas as pd
import json

import psycopg2


def create_connection():
    try:
        cnx = psycopg2.connect(
            host='localhost',
            user=config["user"],
            password=config["password"],
            database=config["database"]
        )
        print('Conexión exitosa!!')
    except psycopg2.Error as e:
        cnx = None
        print('No se puede conectar:', e)
    return cnx

def load(**kwargs):
    ti = kwargs["ti"]
    str_data = ti.xcom_pull(task_ids='transform_carros')
    if str_data is not None:
        json_data = json.loads(str_data)
    else:
        print("Error: No se obtuvo datos válidos de XCom.")
        return
     carga_csv =pd.json_normalize(data=json_data)

    cnx = None
    insert_query = """
    INSERT INTO carros (CRASH_DATE, POSTED_SPEED_LIMIT, WEATHER_CONDITION, CRASH_TYPE, STREET_NO, STREET_NAME, INJURIES_TOTAL, INJURIES_FATAL, INJURIES_INCAPACITATING,INJURIES_NON_INCAPACITATING,INJURIES_REPORTED_NOT_EVIDENT,INJURIES_NO_INDICATION,INJURIES_UNKNOWN,CRASH_HOUR,CRASH_DAY_OF_WEEK,CRASH_MONTH,LATITUDE,LONGITUDE,LOCATION)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s)
     """
    
    try:
        cnx = create_connection()  # Assuming you have a function named create_connection to establish the DB connection
        cur = cnx.cursor()
        for index, row in df.iterrows():
            values = (
                row['CRASH_DATE'], row['POSTED_SPEED_LIMIT'], row['WEATHER_CONDITION'], row['CRASH_TYPE'], row['STREET_NO'],
                row[' STREET_NAME'], row['INJURIES_TOTAL'], row['INJURIES_FATAL'], row['INJURIES_INCAPACITATING'],  row['INJURIES_NON_INCAPACITATING'], row['INJURIES_REPORTED_NOT_EVIDENT'], row['INJURIES_NO_INDICATION'],
                 row['INJURIES_UNKNOWN'], row['CRASH_HOUR'], row[' CRASH_DAY_OF_WEEK'], row['CRASH_MONTH'], row['LATITUDE'], row['LONGITUDE'], row['LOCATION']
            )
            cur.execute(insert_query, values)
        cur.close()
        cnx.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if cnx is not None:
            cnx.close()
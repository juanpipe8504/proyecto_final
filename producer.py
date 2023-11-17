from kafka import KafkaProducer
from json import dumps
import pandas as pd

# Leer el archivo CSV
df_unido = pd.read_csv("datos_unidos.csv", encoding='latin1')

# Renombrar columnas
nuevo_nombre_columnas = {'DescripciÃ³n del Clima': 'Descripción clima'}
df_unido.rename(columns=nuevo_nombre_columnas, inplace=True)

# Convertir Temperatura de Kelvin a Celsius
df_unido['Temperatura (K)'] = pd.to_numeric(df_unido['Temperatura (K)'], errors='coerce')
nuevo_nombre_columnas = {'Temperatura (K)': 'Temperatura (C)'}
df_unido.rename(columns=nuevo_nombre_columnas, inplace=True)
df_unido['Temperatura (C)'] = df_unido['Temperatura (C)'] - 273.15

# Convertir Humedad a numérico
df_unido['Humedad'] = pd.to_numeric(df_unido['Humedad'], errors='coerce')

# Filtrar columnas
columna = 'Descripción clima'
variable_a_eliminar = 'No disponible'
df_unido = df_unido[df_unido[columna] != variable_a_eliminar]

# Función para enviar DataFrame a Kafka
def kafka_producer(df):
    try:
        producer = KafkaProducer(
            value_serializer=lambda m: dumps(m).encode('utf-8'),
            bootstrap_servers=['localhost:9092'],
            acks='all',
            retries=5,
        )

        # Convertir DataFrame a formato JSON
        df_json = df.to_json(orient='records')

        # Enviar el DataFrame como mensaje
        producer.send("kafka1", value={"data": df_json})
        print("DataFrame enviado a Kafka")

    except Exception as e:
        print(f"Error: {e}")

# Llamar a la función con tu DataFrame
kafka_producer(df_unido)
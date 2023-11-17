from kafka import KafkaConsumer
from json import loads
import pandas as pd

def kafka_consumer():
    try:
        consumer = KafkaConsumer(
            'kafka1',  
            auto_offset_reset='earliest',
            enable_auto_commit=True,
            group_id='my-group-1',
            value_deserializer=lambda m: loads(m.decode('utf-8')),
            bootstrap_servers=['localhost:9092']
        )

        print("Consumer connected to Kafka. Waiting for messages...")

        for m in consumer:
            try:
                df_json = m.value['data']
                df_received = pd.read_json(df_json, orient='records')
                print("Received DataFrame from Kafka:")
                print(df_received)

            except Exception as e:
                print(f"Error processing message: {e}")

    except Exception as e:
        print(f"Error connecting to Kafka: {e}")

if __name__ == "__main__":
    kafka_consumer()
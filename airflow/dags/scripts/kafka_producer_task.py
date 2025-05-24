import os
import time
import pandas as pd
from dotenv import load_dotenv
from confluent_kafka import Producer

def kafka_producer_task():
    load_dotenv(dotenv_path="/opt/airflow/.env")
    
    merged_path = os.path.join(os.getenv("OUTPUT"), "merged.csv")
    if not os.path.exists(merged_path):
        raise FileNotFoundError(f"No se encontró el archivo: {merged_path}")

    topic = "traffic_accidents"
    df = pd.read_csv(merged_path)
    
    conf = {
        'bootstrap.servers': 'kafka:9092',
        'queue.buffering.max.messages': 1000000,
        'default.topic.config': {'acks': 'all'}
    }
    producer = Producer(conf)

    def delivery_report(err, msg):
        if err is not None:
            print(f'❌ Error entregando mensaje: {err}')
        else:
            pass  # Mensaje entregado correctamente

    print(f"Total de registros a enviar: {len(df)}")
    
    for idx, row in df.iterrows():
        try:
            producer.produce(topic, value=row.to_json(), callback=delivery_report)
        except BufferError:
            print(f"⚠️ Buffer lleno en mensaje {idx}. Haciendo flush...")
            producer.flush()
            producer.produce(topic, value=row.to_json(), callback=delivery_report)

        if idx % 10000 == 0:
            print(f"🟢 Enviados {idx} mensajes")
            producer.flush()

        time.sleep(0.0005)  # 0.5ms de pausa por mensaje para evitar sobrecarga

    print("todos los mensajes encolados. Haciendo flush final...")
    producer.flush()
    print("envío completo.")

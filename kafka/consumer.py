from confluent_kafka import Consumer
import json
import redis
import os
from dotenv import load_dotenv

load_dotenv()

# Configuración de Redis
redis_client = redis.Redis(host='redis', port=6379, db=0)

# Configuración del Consumer
consumer_conf = {
    'bootstrap.servers': 'kafka:9092',
    'group.id': 'streamlit-consumer',
    'auto.offset.reset': 'earliest',
}

consumer = Consumer(consumer_conf)
consumer.subscribe(['traffic_accidents'])

print("Consumer escuchando el topic 'traffic_accidents'...")

try:
    while True:
        msg = consumer.poll(1.0)  # espera hasta 1 segundo
        if msg is None:
            continue
        if msg.error():
            print(f"error en el mensaje: {msg.error()}")
            continue

        try:
            # Procesar el mensaje recibido
            data = json.loads(msg.value().decode('utf-8'))
            redis_client.rpush("accidents", json.dumps(data))  # Guardar en Redis
        except Exception as e:
            print(f"⚠️ Error procesando mensaje: {e}")

except KeyboardInterrupt:
    print("interrupción manual. Cerrando consumer...")

finally:
    consumer.close()
    print("consumer cerrado correctamente.")

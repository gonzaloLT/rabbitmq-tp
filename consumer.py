import pika
import json
import time

credentials = pika.PlainCredentials('admin', 'admin')

connection = pika.BlockingConnection(
    pika.ConnectionParameters(
        host='localhost',
        credentials=credentials
    )
)

channel = connection.channel()
channel.queue_declare(queue='tareas', durable=True)

channel.basic_qos(prefetch_count=1)

def callback(ch, method, properties, body):
    mensaje = json.loads(body)
    print(f"Procesando: {mensaje}")

    time.sleep(3)

    print(f"Finalizado: {mensaje['id']}")
    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_consume(
    queue='tareas',
    on_message_callback=callback
)

print("Esperando mensajes...")
channel.start_consuming()

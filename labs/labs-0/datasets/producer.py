from confluent_kafka import Producer
import time
import json

config = {
    'bootstrap.servers': 'localhost:9092',
}

producer = Producer(config)
topic = "teste-retencao"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao entregar: {err}")
    else:
        print(f"Mensagem {msg.value().decode('utf-8')} entregue na partição {msg.partition()}")

print("Iniciando envio de mensagens...")

for j in range(1, 20001):
    for i in range(1, 20001):
        data = {"numero": i, "conteudo": f"Lote de teste {i}"}
        payload = json.dumps(data)
        
        # Envia a mensagem
        producer.produce(topic, value=payload, callback=delivery_report)

    # O flush garante que todas as mensagens saiam do buffer para o Kafka
    producer.flush()
    time.sleep(15)

print("Envio finalizado.")
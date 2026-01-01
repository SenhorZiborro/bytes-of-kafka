from confluent_kafka import Consumer, KafkaError
import time
import json

config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'meu-grupo-teste',
    'auto.offset.reset': 'earliest' # Começa do início do tópico
}

consumer = Consumer(config)
consumer.subscribe(["teste-retencao"])

print("Consumidor pronto. Aguardando mensagens...")

try:
    while True:
        msg = consumer.poll(1.0) # Espera 1 segundo por novas mensagens

        if msg is None:
            continue
        if msg.error():
            print(f"Erro no consumidor: {msg.error()}")
            continue

        # Processa a mensagem
        data = json.loads(msg.value().decode('utf-8'))
        print(f"Recebido: Mensagem nº {data['numero']} - Offset: {msg.offset()}")

        # SIMULAÇÃO DE LENTIDÃO: Espera 1 segundo por mensagem
        print("Processando... (dormindo 5s)")
        time.sleep(5)

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
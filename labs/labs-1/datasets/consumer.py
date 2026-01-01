from confluent_kafka import Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import time

# 1. Configuração do Schema Registry
sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

# 2. Configuração do Deserializador (CORRIGIDO)
# Usamos argumentos nomeados para garantir que o cliente seja passado no lugar certo
json_deserializer = JSONDeserializer(
    schema_str=None, 
    schema_registry_client=schema_registry_client
) 

# 3. Configuração do Consumidor Kafka
config = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'meu-grupo-json-schema',
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
topic = "teste-retencao"
consumer.subscribe([topic])

print(f"Consumidor pronto (JSON Schema) no tópico: {topic}")
print("Aguardando mensagens...")

try:
    while True:
        msg = consumer.poll(1.0) 

        if msg is None:
            continue
        if msg.error():
            print(f"Erro no consumidor: {msg.error()}")
            continue

        try:
            # 4. Deserialização da mensagem
            # O contexto informa que o schema está no VALOR (payload) da mensagem
            data = json_deserializer(
                msg.value(), 
                SerializationContext(msg.topic(), MessageField.VALUE)
            )
            
            print(f"--- Mensagem Recebida ---")
            print(f"Offset: {msg.offset()} | Dados: {data}")
            
            # Simulação de processamento
            print("Processando... (dormindo 2s)")
            time.sleep(2)

        except Exception as e:
            print(f"Erro ao deserializar no offset {msg.offset()}: {e}")

except KeyboardInterrupt:
    print("\nEncerrando consumidor...")
finally:
    consumer.close()
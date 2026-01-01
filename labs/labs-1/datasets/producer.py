from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONSerializer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
import time

# 1. Definição do Schema JSON (Contrato)
schema_str = """
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "User",
  "type": "object",
  "properties": {
    "numero": { "type": "integer" },
    "conteudo": { "type": "string" }
  },
  "required": ["numero", "conteudo"]
}
"""

# 2. Configuração do Schema Registry
sr_config = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(sr_config)

# 3. Configuração do Serializador JSON
json_serializer = JSONSerializer(schema_str, schema_registry_client)
string_serializer = StringSerializer('utf_8')

# 4. Configuração do Produtor
producer_config = {'bootstrap.servers': 'localhost:9092'}
producer = Producer(producer_config)
topic = "teste-retencao"

def delivery_report(err, msg):
    if err is not None:
        print(f"Erro ao entregar: {err}")
    else:
        print(f"Mensagem {msg.value()} entregue no offset {msg.offset()}")

print("Iniciando envio com JSON Schema...")

try:
    for j in range(1, 20001):
        for i in range(1, 10): # Reduzi para teste rápido, ajuste como quiser
            data = {"numero": i, "conteudo": f"Lote de teste {i}"}
            
            # O Serializer agora cuida da validação e do registro no Registry
            producer.produce(
                topic=topic,
                value=json_serializer(data, SerializationContext(topic, MessageField.VALUE)),
                callback=delivery_report
            )
        producer.flush()
        print(f"Lote {j} enviado. Dormindo 5s (Retenção vai agir!)...")
        time.sleep(5)
except KeyboardInterrupt:
    pass
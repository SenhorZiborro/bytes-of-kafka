# 1 - Schema Registry

Instale as bibliotecas necessárias

```sh
pip install confluent-kafka[json]
```

Altere o *schema* usado para estudar outros tipos de dados e formatos de dados. Modifique o produtor e consumidor para gerar erros e entender o comportamento das tecnologias.

```json
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
```

Exemplos de como você pode interagir com a API web do schema registry

```sh
curl -s http://localhost:8081/subjects | jq

curl -s http://localhost:8081/schemas | jq

curl -s http://localhost:8081/schemas/ids/1 | jq
```
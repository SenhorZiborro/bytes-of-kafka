# 1 - Kafka Retention Policy


Instale as bibliotecas necessárias

```sh
pip install confluent-kafka
```

Altere os parâmetros do *docker-compose* para estudar sobre a politica de retenção. Modifique o produtor e consumidor para analisar o comportamento deles.

```sh
KAFKA_LOG_RETENTION_MS: 10000
KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 2000
KAFKA_LOG_SEGMENT_BYTES: 1048576
```
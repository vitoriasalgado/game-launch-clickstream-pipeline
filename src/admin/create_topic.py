import os

import yaml
from confluent_kafka.admin import AdminClient, NewTopic

bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

config_kafka = {"bootstrap.servers": bootstrap_servers}

admin = AdminClient(config_kafka)
metadados = admin.list_topics(timeout=10)
print(metadados.topics)

with open("config/topics.yaml", "r", encoding="utf-8") as arquivo:
    conteudo = arquivo.read()
    configuracao = yaml.safe_load(conteudo)
    topicos = configuracao["topics"]
    topico = topicos[0]
    print(topico)

nome_topico = topico["name"]
existe = nome_topico in metadados.topics
if existe:
    print("O tópico já existe.")
else:
    print("O tópico ainda precisa ser criado.")
    novo_topico = NewTopic(
        topic=nome_topico,
        num_partitions=topico["partitions"],
        replication_factor=topico["replication_factor"],
        config={"retention.ms": str(topico["retention_ms"])},
    )
    criacao_por_topico = admin.create_topics([novo_topico], request_timeout=10)
    criacao = criacao_por_topico[nome_topico]
    criacao.result()
    print("Tópico criado com sucesso.")

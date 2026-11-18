import os

import yaml
from confluent_kafka import Consumer, KafkaException


def principal() -> None:

    endereco_kafka = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    configuracao_consumer = {
        "bootstrap.servers": endereco_kafka,
        "group.id": "minimal-consumer",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    }

    with open("config/topics.yaml", "r", encoding="utf-8") as arquivo:
        conteudo = arquivo.read()
        configuracao_topicos = yaml.safe_load(conteudo)
        topicos = configuracao_topicos["topics"]
        topico = topicos[0]
        nome_topico = topico["name"]

    consumer = Consumer(configuracao_consumer)

    try:
        consumer.subscribe([nome_topico])

        while True:
            mensagem = consumer.poll(timeout=1.0)
            if mensagem is None:
                continue
            if mensagem.error():
                raise KafkaException(mensagem.error())
            print(mensagem.partition(), mensagem.offset(), mensagem.value())
            consumer.commit(message=mensagem, asynchronous=False)

    except KeyboardInterrupt:
        print("Consumo interrompido.")

    finally:
        consumer.close()


if __name__ == "__main__":
    principal()

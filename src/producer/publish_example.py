import json
import os
import uuid
from datetime import UTC, datetime

import yaml
from confluent_kafka import Producer

TIPOS_EVENTOS = [
    "game_page_view",
    "trailer_play",
    "wishlist_added",
    "edition_compared",
    "preorder_checkout_started",
    "preorder_completed",
]


def criar_evento(
    tipo_evento="game_page_view",
    usuario_id="user_exemplo_01",
    sessao_id="session_exemplo_01",
):

    if tipo_evento not in TIPOS_EVENTOS:
        raise ValueError("Tipo de evento inválido.")

    evento = {
        "event_id": str(uuid.uuid4()),
        "occurred_at": datetime.now(UTC).isoformat(),
        "event_type": tipo_evento,
        "user_id": usuario_id,
        "session_id": sessao_id,
        "campaign_id": "campanha_sintetica_01",
        "source": "synthetic-generator",
    }
    return evento


def confirmar_entrega(erro, mensagem_kafka):
    if erro:
        print(erro)
    else:
        print("Evento entregue ao Kafka.")


def principal():

    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    config_kafka = {
        "bootstrap.servers": bootstrap_servers,
    }

    produtor = Producer(config_kafka)

    with open("config/topics.yaml", "r", encoding="utf-8") as arquivo:
        conteudo = arquivo.read()
        configuracao = yaml.safe_load(conteudo)
        topicos = configuracao["topics"]
        topico = topicos[0]
        nome_topico = topico["name"]

        print(nome_topico)

    evento = criar_evento()

    mensagem = json.dumps(evento)
    print(mensagem)
    produtor.produce(
        topic=nome_topico,
        key=evento["session_id"],
        value=mensagem,
        on_delivery=confirmar_entrega,
    )
    pendentes = produtor.flush(10)
    print(pendentes)


if __name__ == "__main__":
    principal()

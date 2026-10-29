import os
import random

import yaml

from src.producer.publish_example import criar_evento


def sortear_evento(sessoes, pesos_tipos):
    sessao_escolhida = random.choice(sessoes)
    tipos = list(pesos_tipos.keys())
    pesos = list(pesos_tipos.values())

    tipo_escolhido = random.choices(tipos, weights=pesos, k=1)[0]

    evento = criar_evento(
        tipo_evento=tipo_escolhido,
        usuario_id=sessao_escolhida["user_id"],
        sessao_id=sessao_escolhida["session_id"],
    )
    return evento


def principal():
    nome_perfil = os.environ.get("LOAD_PROFILE", "normal")

    with open("config/load-profiles.yaml", "r", encoding="utf-8") as arquivo:
        conteudo = arquivo.read()
        configuracao = yaml.safe_load(conteudo)

    perfil = configuracao[nome_perfil]
    taxa_alvo = perfil["target_events_per_second"]
    duracao_segundos = perfil["duration_seconds"]
    quantidade_sessoes = perfil["active_sessions"]

    print(nome_perfil, taxa_alvo, duracao_segundos, quantidade_sessoes)

    sessoes = []
    for indice in range(quantidade_sessoes):
        sessao = {"user_id": f"usuario_{indice}", "session_id": f"sessao_{indice}"}
        sessoes.append(sessao)

    print(len(sessoes))

    evento = sortear_evento(sessoes, perfil["event_type_weights"])

    print(evento)


if __name__ == "__main__":
    principal()

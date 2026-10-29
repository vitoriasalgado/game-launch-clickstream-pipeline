import json
import math
import os
import random
import time

import yaml
from confluent_kafka import Producer

from src.producer.publish_example import TIPOS_EVENTOS, criar_evento


def validar_perfil(perfil: dict) -> None:
    for campo in (
        "target_events_per_second",
        "duration_seconds",
        "flush_timeout_seconds",
    ):
        valor = perfil.get(campo)
        if (
            isinstance(valor, bool)
            or not isinstance(valor, (int, float))
            or not math.isfinite(valor)
            or valor <= 0
        ):
            raise ValueError(f"{campo} deve ser um número positivo e finito.")

    for campo in ("active_sessions", "delivery_timeout_ms"):
        valor = perfil.get(campo)
        if isinstance(valor, bool) or not isinstance(valor, int) or valor <= 0:
            raise ValueError(f"{campo} deve ser um inteiro positivo.")

    pesos = perfil.get("event_type_weights")
    if not isinstance(pesos, dict) or set(pesos) != set(TIPOS_EVENTOS):
        raise ValueError("event_type_weights deve conter os seis tipos do contrato.")
    for peso in pesos.values():
        if (
            isinstance(peso, bool)
            or not isinstance(peso, (int, float))
            or not math.isfinite(peso)
            or peso < 0
        ):
            raise ValueError("Os pesos devem ser números não negativos e finitos.")
    if not math.isfinite(sum(pesos.values())) or sum(pesos.values()) <= 0:
        raise ValueError("A soma dos pesos deve ser positiva e finita.")


def carregar_perfil(nome: str, caminho: str = "config/load-profiles.yaml") -> dict:
    with open(caminho, "r", encoding="utf-8") as arquivo:
        configuracao = yaml.safe_load(arquivo)
    if not isinstance(configuracao, dict) or nome not in configuracao:
        raise ValueError(f"Perfil de carga inexistente: {nome}")
    perfil = configuracao[nome]
    if not isinstance(perfil, dict):
        raise TypeError(f"Configuração inválida para o perfil: {nome}")
    validar_perfil(perfil)
    return perfil


def criar_sessoes(quantidade: int) -> list[dict[str, str]]:
    sessoes = []
    for indice in range(quantidade):
        sessoes.append(
            {"user_id": f"usuario_{indice}", "session_id": f"sessao_{indice}"}
        )
    return sessoes


def sortear_evento(sessoes: list[dict[str, str]], pesos_tipos: dict) -> dict:
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


def executar_carga(
    produtor: Producer, nome_topico: str, perfil: dict, sessoes: list[dict[str, str]]
) -> dict:
    validar_perfil(perfil)
    if not sessoes:
        raise ValueError("É necessário fornecer pelo menos uma sessão.")

    contagem = {
        "gerados": 0,
        "enfileirados": 0,
        "entregues": 0,
        "falhas_entrega": 0,
        "esperas_fila_cheia": 0,
        "primeiro_erro": None,
    }

    def confirmar_entrega(erro, mensagem_kafka):
        if erro is None:
            contagem["entregues"] += 1
        else:
            contagem["falhas_entrega"] += 1
            if contagem["primeiro_erro"] is None:
                contagem["primeiro_erro"] = str(erro)

    inicio = time.monotonic()
    fim = inicio + perfil["duration_seconds"]
    try:
        while time.monotonic() < fim:
            proximo_envio = inicio + (
                contagem["enfileirados"] / perfil["target_events_per_second"]
            )
            espera = min(proximo_envio, fim) - time.monotonic()
            if espera > 0:
                time.sleep(espera)
            if time.monotonic() >= fim:
                break

            evento = sortear_evento(sessoes, perfil["event_type_weights"])
            mensagem = json.dumps(evento)
            contagem["gerados"] += 1
            while time.monotonic() < fim:
                try:
                    produtor.produce(
                        topic=nome_topico,
                        key=evento["session_id"],
                        value=mensagem,
                        on_delivery=confirmar_entrega,
                    )
                except BufferError:
                    contagem["esperas_fila_cheia"] += 1
                    # Processa confirmações e espera espaço sem ultrapassar a janela.
                    produtor.poll(min(0.01, max(0, fim - time.monotonic())))
                else:
                    contagem["enfileirados"] += 1
                    break
            produtor.poll(0)
    finally:
        tempo_geracao = time.monotonic() - inicio
        # Uma única drenagem final, sem bloquear a cada mensagem publicada.
        pendentes = produtor.flush(perfil["flush_timeout_seconds"])

    tempo_total = time.monotonic() - inicio
    return {
        **contagem,
        "nao_enfileirados": contagem["gerados"] - contagem["enfileirados"],
        "pendentes": pendentes,
        "taxa_alvo_eventos_s": perfil["target_events_per_second"],
        "duracao_configurada_s": perfil["duration_seconds"],
        "tempo_geracao_s": tempo_geracao,
        "tempo_drenagem_s": tempo_total - tempo_geracao,
        "tempo_total_s": tempo_total,
        "taxa_enfileiramento_observada_eventos_s": (
            contagem["enfileirados"] / tempo_geracao
        ),
        "taxa_entrega_observada_eventos_s": contagem["entregues"] / tempo_total,
    }


def principal() -> int:
    nome_perfil = os.environ.get("LOAD_PROFILE", "normal")
    perfil = carregar_perfil(nome_perfil)
    bootstrap_servers = os.environ["KAFKA_BOOTSTRAP_SERVERS"]

    with open("config/topics.yaml", "r", encoding="utf-8") as arquivo:
        configuracao_topicos = yaml.safe_load(arquivo)
    nome_topico = configuracao_topicos["topics"][0]["name"]

    produtor = Producer(
        {
            "bootstrap.servers": bootstrap_servers,
            "enable.idempotence": True,
            "allow.auto.create.topics": False,
            "message.timeout.ms": perfil["delivery_timeout_ms"],
        }
    )
    sessoes = criar_sessoes(perfil["active_sessions"])
    resumo = executar_carga(produtor, nome_topico, perfil, sessoes)
    resumo.update(
        {
            "carga_sintetica": True,
            "perfil": nome_perfil,
            "topico": nome_topico,
            "sessoes_sinteticas": len(sessoes),
        }
    )
    print(json.dumps(resumo, ensure_ascii=False, indent=2))
    return int(
        resumo["falhas_entrega"] > 0
        or resumo["pendentes"] > 0
        or resumo["nao_enfileirados"] > 0
    )


if __name__ == "__main__":
    raise SystemExit(principal())

from unittest.mock import Mock, call

import pytest
from confluent_kafka import KafkaError, KafkaException

from src.consumers import consumir_eventos


@pytest.fixture
def consumer_simulado(monkeypatch):
    consumer = Mock(spec=["subscribe", "poll", "commit", "close"])
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka-teste:9092")
    monkeypatch.setattr(consumir_eventos, "Consumer", Mock(return_value=consumer))
    return consumer


@pytest.fixture
def mensagem():
    mensagem = Mock(spec=["error", "partition", "offset", "value"])
    mensagem.error.return_value = None
    mensagem.partition.return_value = 0
    mensagem.offset.return_value = 10
    mensagem.value.return_value = b"evento sintetico"
    return mensagem


def test_confirma_offset_apos_processar(monkeypatch, consumer_simulado, mensagem):
    consumer_simulado.poll.side_effect = [None, mensagem, KeyboardInterrupt()]
    imprimir = Mock()
    monkeypatch.setattr(consumir_eventos, "print", imprimir, raising=False)
    ordem = Mock()
    ordem.attach_mock(imprimir, "processar")
    ordem.attach_mock(consumer_simulado.commit, "confirmar")

    consumir_eventos.principal()

    ordem.assert_has_calls(
        [
            call.processar(0, 10, b"evento sintetico"),
            call.confirmar(message=mensagem, asynchronous=False),
        ]
    )
    consumer_simulado.commit.assert_called_once_with(
        message=mensagem, asynchronous=False
    )
    configuracao = consumir_eventos.Consumer.call_args.args[0]
    assert configuracao["enable.auto.commit"] is False
    assert configuracao["enable.auto.offset.store"] is False
    consumer_simulado.close.assert_called_once_with()


def test_nao_confirma_offset_se_processamento_falhar(
    monkeypatch, consumer_simulado, mensagem
):
    consumer_simulado.poll.side_effect = [mensagem, KeyboardInterrupt()]
    imprimir = Mock(side_effect=OSError("Falha na saida do terminal"))
    monkeypatch.setattr(consumir_eventos, "print", imprimir, raising=False)

    with pytest.raises(OSError, match="Falha na saida do terminal"):
        consumir_eventos.principal()

    imprimir.assert_called_once_with(0, 10, b"evento sintetico")
    consumer_simulado.commit.assert_not_called()
    consumer_simulado.close.assert_called_once_with()


def test_nao_processa_nem_confirma_mensagem_com_erro(
    consumer_simulado, mensagem, capsys
):
    mensagem.error.return_value = KafkaError(KafkaError._BAD_MSG, "Mensagem invalida")
    consumer_simulado.poll.side_effect = [mensagem, KeyboardInterrupt()]

    with pytest.raises(KafkaException):
        consumir_eventos.principal()

    assert capsys.readouterr().out == ""
    consumer_simulado.commit.assert_not_called()
    consumer_simulado.close.assert_called_once_with()


@pytest.mark.parametrize("operacao", ["poll", "commit"])
def test_fecha_consumer_e_propaga_falha(consumer_simulado, mensagem, operacao):
    consumer_simulado.poll.side_effect = [mensagem, KeyboardInterrupt()]
    getattr(consumer_simulado, operacao).side_effect = RuntimeError("Falha simulada")

    with pytest.raises(RuntimeError, match="Falha simulada"):
        consumir_eventos.principal()

    if operacao == "poll":
        consumer_simulado.commit.assert_not_called()
    else:
        consumer_simulado.commit.assert_called_once_with(
            message=mensagem, asynchronous=False
        )
    consumer_simulado.close.assert_called_once_with()


def test_fecha_consumer_ao_interromper_sem_mensagens(consumer_simulado):
    consumer_simulado.poll.side_effect = [KeyboardInterrupt]
    consumir_eventos.principal()

    consumer_simulado.commit.assert_not_called()

    consumer_simulado.close.assert_called_once_with()

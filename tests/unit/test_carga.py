import json

import pytest

from src.producer import gerar_eventos


class RelogioSimulado:
    def __init__(self):
        self.agora = 0.0

    def monotonic(self):
        return self.agora

    def sleep(self, segundos):
        assert segundos >= 0
        self.agora += segundos


class ProdutorSimulado:
    def __init__(
        self,
        relogio,
        atraso_final=0,
        erro=None,
        fila_cheia_vezes=0,
        fila_sempre_cheia=False,
        confirmar_no_poll=False,
    ):
        self.relogio = relogio
        self.atraso_final = atraso_final
        self.erro = erro
        self.fila_cheia_vezes = fila_cheia_vezes
        self.fila_sempre_cheia = fila_sempre_cheia
        self.confirmar_no_poll = confirmar_no_poll
        self.mensagens = []
        self.tentativas = []
        self.confirmacoes = []
        self.limite_flush = None

    def produce(self, **argumentos):
        self.tentativas.append(argumentos)
        if self.fila_sempre_cheia or self.fila_cheia_vezes > 0:
            self.fila_cheia_vezes -= 1
            raise BufferError("Fila cheia")
        self.mensagens.append(argumentos)
        self.confirmacoes.append(argumentos["on_delivery"])

    def confirmar(self):
        for callback in self.confirmacoes:
            callback(self.erro, None)
        self.confirmacoes.clear()

    def poll(self, espera):
        self.relogio.sleep(espera)
        if self.confirmar_no_poll:
            self.confirmar()

    def flush(self, limite):
        self.limite_flush = limite
        self.relogio.sleep(min(self.atraso_final, limite))
        if self.atraso_final <= limite:
            self.confirmar()
        return len(self.confirmacoes)


@pytest.fixture
def perfil():
    configuracao = gerar_eventos.carregar_perfil("normal")
    configuracao.update(
        target_events_per_second=4,
        duration_seconds=1,
        active_sessions=2,
        flush_timeout_seconds=2,
    )
    return configuracao


@pytest.fixture
def relogio(monkeypatch):
    simulado = RelogioSimulado()
    monkeypatch.setattr(gerar_eventos, "time", simulado)
    return simulado


def test_taxa_chave_e_confirmacoes(perfil, relogio):
    produtor = ProdutorSimulado(relogio, confirmar_no_poll=True)
    sessoes = gerar_eventos.criar_sessoes(2)

    resumo = gerar_eventos.executar_carga(produtor, "topico_teste", perfil, sessoes)

    assert resumo["gerados"] == resumo["enfileirados"] == resumo["entregues"] == 4
    assert resumo["falhas_entrega"] == resumo["pendentes"] == 0
    assert resumo["tempo_geracao_s"] == pytest.approx(1)
    assert resumo["taxa_enfileiramento_observada_eventos_s"] == pytest.approx(4)
    assert resumo["taxa_entrega_observada_eventos_s"] == pytest.approx(4)
    assert (
        len({json.loads(item["value"])["event_id"] for item in produtor.mensagens}) == 4
    )
    for item in produtor.mensagens:
        evento = json.loads(item["value"])
        assert item["topic"] == "topico_teste"
        assert item["key"] == evento["session_id"]
        assert evento["event_type"] in gerar_eventos.TIPOS_EVENTOS
        assert evento["source"] == "synthetic-generator"
        assert any(
            sessao["session_id"] == evento["session_id"]
            and sessao["user_id"] == evento["user_id"]
            for sessao in sessoes
        )


def test_taxa_inclui_drenagem_final(perfil, relogio):
    produtor = ProdutorSimulado(relogio, atraso_final=1)

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["entregues"] == 4
    assert resumo["tempo_drenagem_s"] == pytest.approx(1)
    assert resumo["tempo_total_s"] == pytest.approx(2)
    assert resumo["taxa_entrega_observada_eventos_s"] == pytest.approx(2)
    assert produtor.limite_flush == 2


def test_produtor_lento_reduz_taxa_observada(monkeypatch, perfil, relogio):
    produtor = ProdutorSimulado(relogio)
    publicar = produtor.produce

    def publicar_lentamente(**argumentos):
        relogio.sleep(0.6)
        publicar(**argumentos)

    monkeypatch.setattr(produtor, "produce", publicar_lentamente)
    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["entregues"] == 2
    assert resumo["tempo_geracao_s"] == pytest.approx(1.2)
    assert resumo["taxa_entrega_observada_eventos_s"] == pytest.approx(2 / 1.2)
    assert resumo["taxa_entrega_observada_eventos_s"] < resumo["taxa_alvo_eventos_s"]


def test_espera_nao_ultrapassa_fim_da_janela(perfil, relogio):
    perfil.update(target_events_per_second=2, duration_seconds=0.1)
    produtor = ProdutorSimulado(relogio)

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["entregues"] == 1
    assert resumo["tempo_geracao_s"] == pytest.approx(0.1)


def test_falha_nao_conta_como_entrega(perfil, relogio):
    produtor = ProdutorSimulado(relogio, erro=RuntimeError("Falha simulada"))

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["enfileirados"] == resumo["falhas_entrega"] == 4
    assert resumo["entregues"] == resumo["pendentes"] == 0
    assert resumo["taxa_entrega_observada_eventos_s"] == 0
    assert resumo["primeiro_erro"] == "Falha simulada"


def test_fila_cheia_repete_o_mesmo_evento(perfil, relogio):
    produtor = ProdutorSimulado(relogio, fila_cheia_vezes=1)

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["esperas_fila_cheia"] == 1
    assert resumo["enfileirados"] == resumo["entregues"] == 4
    assert produtor.tentativas[0]["value"] == produtor.tentativas[1]["value"]
    assert resumo["nao_enfileirados"] == 0


def test_fila_cheia_respeita_duracao(perfil, relogio):
    produtor = ProdutorSimulado(relogio, fila_sempre_cheia=True)

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["gerados"] == resumo["nao_enfileirados"] == 1
    assert resumo["enfileirados"] == resumo["entregues"] == 0
    assert resumo["tempo_geracao_s"] == pytest.approx(1)


def test_flush_informa_pendencias(perfil, relogio):
    produtor = ProdutorSimulado(relogio, atraso_final=10)

    resumo = gerar_eventos.executar_carga(
        produtor, "topico_teste", perfil, gerar_eventos.criar_sessoes(2)
    )

    assert resumo["pendentes"] == resumo["enfileirados"] == 4
    assert resumo["entregues"] == resumo["falhas_entrega"] == 0
    assert resumo["tempo_drenagem_s"] == pytest.approx(2)


@pytest.mark.parametrize(
    ("campo", "valor"),
    [
        ("target_events_per_second", 0),
        ("target_events_per_second", float("nan")),
        ("target_events_per_second", True),
        ("duration_seconds", -1),
        ("active_sessions", 0),
        ("active_sessions", 1.5),
        ("delivery_timeout_ms", 0),
        ("flush_timeout_seconds", 0),
    ],
)
def test_rejeita_perfil_invalido(perfil, campo, valor):
    perfil[campo] = valor
    with pytest.raises(ValueError):
        gerar_eventos.validar_perfil(perfil)


@pytest.mark.parametrize("peso", [-1, float("inf"), True])
def test_rejeita_peso_invalido(perfil, peso):
    perfil["event_type_weights"]["game_page_view"] = peso
    with pytest.raises(ValueError):
        gerar_eventos.validar_perfil(perfil)


def test_rejeita_pesos_zerados(perfil):
    perfil["event_type_weights"] = dict.fromkeys(gerar_eventos.TIPOS_EVENTOS, 0)
    with pytest.raises(ValueError):
        gerar_eventos.validar_perfil(perfil)


def test_rejeita_tipo_desconhecido(perfil):
    perfil["event_type_weights"]["desconhecido"] = 1
    with pytest.raises(ValueError):
        gerar_eventos.validar_perfil(perfil)


def test_rejeita_perfil_inexistente():
    with pytest.raises(ValueError, match="inexistente"):
        gerar_eventos.carregar_perfil("inexistente")


def test_rejeita_lista_de_sessoes_vazia(perfil, relogio):
    produtor = ProdutorSimulado(relogio)
    with pytest.raises(ValueError, match="sessão"):
        gerar_eventos.executar_carga(produtor, "topico_teste", perfil, [])


@pytest.mark.parametrize("resultado", ["sucesso", "falha", "pendente", "fila_cheia"])
def test_principal_indica_status_sem_kafka(
    monkeypatch, perfil, relogio, resultado, capsys
):
    produtor = ProdutorSimulado(
        relogio,
        erro=RuntimeError("Falha") if resultado == "falha" else None,
        atraso_final=10 if resultado == "pendente" else 0,
        fila_sempre_cheia=resultado == "fila_cheia",
    )
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker_teste:9092")
    monkeypatch.setenv("LOAD_PROFILE", "normal")
    monkeypatch.setattr(gerar_eventos, "carregar_perfil", lambda nome: perfil)
    monkeypatch.setattr(gerar_eventos, "Producer", lambda configuracao: produtor)

    codigo = gerar_eventos.principal()

    resumo = json.loads(capsys.readouterr().out)
    assert codigo == int(resultado != "sucesso")
    assert resumo["carga_sintetica"] is True
    assert resumo["perfil"] == "normal"
    assert resumo["entregues"] == (4 if resultado == "sucesso" else 0)

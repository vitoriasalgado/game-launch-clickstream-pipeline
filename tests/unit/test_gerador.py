import yaml

from src.producer.gerar_eventos import sortear_evento


def test_sorteio():
    sessoes = [{"user_id": "usuario_teste", "session_id": "sessao_teste"}]
    pesos = {"game_page_view": 0, "trailer_play": 1}

    evento = sortear_evento(sessoes, pesos)

    assert evento["event_type"] == "trailer_play"
    assert evento["user_id"] == "usuario_teste"
    assert evento["session_id"] == "sessao_teste"


def test_perfis():
    with open("config/load-profiles.yaml", "r", encoding="utf-8") as arquivo:
        configuracao = yaml.safe_load(arquivo)

    nomes = ["normal", "campaign_trailer", "viral_peak"]
    for nome in nomes:
        assert nome in configuracao
        perfil = configuracao[nome]
        assert perfil["target_events_per_second"] > 0
        assert perfil["duration_seconds"] > 0
        assert perfil["active_sessions"] > 0
        assert len(perfil["event_type_weights"]) == 6

from datetime import datetime, timedelta
from uuid import UUID

import pytest

from src.producer.publish_example import criar_evento


def test_evento():
    evento = criar_evento()
    assert "event_id" in evento
    assert "occurred_at" in evento
    assert "event_type" in evento
    assert "user_id" in evento
    assert "session_id" in evento
    assert "campaign_id" in evento
    assert "source" in evento

    assert evento["event_type"] == "game_page_view"
    assert evento["source"] == "synthetic-generator"


def test_identificador():
    primeiro = criar_evento()
    segundo = criar_evento()

    UUID(primeiro["event_id"])

    assert primeiro["event_id"] != segundo["event_id"]


def test_horario_utc():
    evento = criar_evento()

    horario = datetime.fromisoformat(evento["occurred_at"])
    assert horario.tzinfo is not None
    assert horario.utcoffset() == timedelta(0)


def test_tipos_permitidos():

    tipos = [
        "trailer_play",
        "game_page_view",
        "wishlist_added",
        "edition_compared",
        "preorder_checkout_started",
        "preorder_completed",
    ]
    for tipo in tipos:
        evento = criar_evento(tipo)
        assert evento["event_type"] == tipo


def test_tipo_invalido():
    with pytest.raises(ValueError):
        criar_evento("tipo_inexistente")

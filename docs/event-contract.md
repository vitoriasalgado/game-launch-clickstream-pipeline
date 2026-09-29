# Contrato de evento

Todos os eventos são sintéticos e serializados inicialmente como JSON.

```json
{
  "event_id": "uuid",
  "event_type": "game_page_view",
  "occurred_at": "2026-09-28T12:00:00Z",
  "user_id": "user_123",
  "session_id": "session_456",
  "campaign_id": "trailer_wave_01",
  "page": "game-detail",
  "edition": "standard",
  "source": "synthetic-generator"
}
```

## Tipos permitidos

- `game_page_view`
- `trailer_play`
- `wishlist_added`
- `edition_compared`
- `preorder_checkout_started`
- `preorder_completed`

## Regras

- A chave Kafka é `session_id`.
- A escolha preserva ordem somente dentro da mesma sessão. Eventos de sessões distintas de um mesmo usuário podem ser processados em partitions diferentes e não têm ordenação relativa garantida.
- `event_id` é único e permite deduplicação futura.
- `occurred_at` usa UTC no formato ISO 8601.
- Campos de negócio adicionais devem ser opcionais para manter compatibilidade do contrato.

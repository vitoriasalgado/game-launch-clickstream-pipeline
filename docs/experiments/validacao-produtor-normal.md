# Validação do produtor — perfil normal

## Contexto e objetivo

Execução local em **28/10/2026**, com dados inteiramente sintéticos. O objetivo foi verificar a publicação contínua no Kafka com o perfil `normal`, comparar a taxa alvo com a taxa observada e conferir as confirmações de entrega.

Os números abaixo foram obtidos no resumo JSON exibido no terminal ao final da execução. Não representam tráfego real, previsão de demanda ou sistemas da Rockstar Games/GTA VI.

## Configuração

| Parâmetro | Valor |
| --- | --- |
| Ambiente | MacBook com Kafka em container local; recursos de hardware não registrados nesta execução |
| Perfil sintético | `normal` |
| Tópico | `game-clickstream-events` |
| Partitions configuradas | 6 |
| Fator de replicação configurado | 1 |
| Chave Kafka | `session_id` |
| Taxa alvo | 500 eventos/s |
| Janela configurada de geração | 10 segundos |
| Sessões sintéticas | 100 |
| Prazo de entrega por mensagem | 10.000 ms |
| Espera máxima de drenagem final | 15 segundos |

Pesos configurados: `game_page_view` 50, `trailer_play` 25, `wishlist_added` 10, `edition_compared` 8, `preorder_checkout_started` 5 e `preorder_completed` 2. São pesos relativos de sorteio; a distribuição efetivamente publicada por tipo não foi registrada neste resumo.

Nenhum consumer da aplicação fez parte do procedimento de validação. A quantidade de consumers por grupo e os demais grupos eventualmente existentes no cluster não foram inspecionados nesta execução.

## Procedimento de reprodução

Com o ambiente virtual ativo, o Kafka rodando e o tópico já criado com a configuração acima, executar na raiz do repositório:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOAD_PROFILE=normal
python -m src.producer.gerar_eventos
```

O programa lê `config/load-profiles.yaml` e `config/topics.yaml`, tenta acompanhar a taxa configurada durante a janela de geração e aguarda as confirmações no período de drenagem final. Uma nova execução deve ter seu próprio resumo registrado; não há garantia de repetir exatamente as medições abaixo.

## Medições observadas

| Métrica | Resultado |
| --- | ---: |
| Eventos gerados | 5.000 |
| Eventos enfileirados no cliente | 5.000 |
| Entregas confirmadas pelo Kafka | 5.000 |
| Falhas de entrega | 0 |
| Eventos não enfileirados | 0 |
| Mensagens pendentes ao final | 0 |
| Esperas por fila local cheia | 0 |
| Tempo de geração | 10,000288417038973 s |
| Tempo de drenagem | 0,000617499987129122 s |
| Tempo total | 10,000905917026103 s |
| Taxa de enfileiramento observada | 499,98557956396127 eventos/s |
| Taxa de entrega observada | 499,95470825175147 eventos/s |

A taxa de enfileiramento divide os eventos aceitos na fila local pelo tempo de geração. A taxa de entrega divide as confirmações de sucesso pelo tempo total, incluindo a drenagem. A taxa alvo é uma configuração; não substitui essas medições.

## Resumo original da execução

```json
{
  "gerados": 5000,
  "enfileirados": 5000,
  "entregues": 5000,
  "falhas_entrega": 0,
  "esperas_fila_cheia": 0,
  "primeiro_erro": null,
  "nao_enfileirados": 0,
  "pendentes": 0,
  "taxa_alvo_eventos_s": 500,
  "duracao_configurada_s": 10,
  "tempo_geracao_s": 10.000288417038973,
  "tempo_drenagem_s": 0.000617499987129122,
  "tempo_total_s": 10.000905917026103,
  "taxa_enfileiramento_observada_eventos_s": 499.98557956396127,
  "taxa_entrega_observada_eventos_s": 499.95470825175147,
  "carga_sintetica": true,
  "perfil": "normal",
  "topico": "game-clickstream-events",
  "sessoes_sinteticas": 100
}
```

## Limitações e medições pendentes

- Foi registrada uma execução de dez segundos; não é um teste de estabilidade prolongada nem uma determinação da capacidade máxima do computador.
- Os perfis `campaign_trailer` e `viral_peak` não foram validados por este resultado.
- Throughput de consumo, consumer lag por grupo/partition, formação e recuperação de backlog não foram medidos. Não há resultado de tempo de recuperação nesta validação.
- CPU e memória dos containers não foram coletadas.
- Latência individual de entrega, processamento e tempo de fila no broker não foram medidos. O tempo de drenagem não representa essas latências.
- Confirmação de publicação não comprova consumo, processamento ou persistência no PostgreSQL.
- O resumo não informa quantos eventos foram gravados em cada partition nem a proporção observada de cada tipo de evento.

## Conclusão

O caminho produtor–Kafka sustentou o perfil normal durante os dez segundos, com 5.000 entregas confirmadas e taxa observada de 499,95 eventos/s para uma meta de 500 eventos/s, sem falhas ou pendências ao final. Não houve espera por espaço na fila local do producer. O resultado valida a publicação nesse cenário, mas ainda não demonstra a capacidade máxima, a estabilidade prolongada ou o comportamento do consumo.

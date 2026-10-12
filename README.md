# game-launch-clickstream-pipeline

Estudo reproduzível de planejamento de capacidade com um pipeline de clickstream inteiramente sintético, inspirado em uma campanha fictícia de pré-venda de GTA VI. Usa Kafka para simular e analisar picos de tráfego, monitorar consumer lag e experimentar a escala de consumidores.

> **Nota de transparência:** este é um projeto de simulação. Todos os eventos, volumes, campanhas e métricas são sintéticos; não representam dados, previsões ou sistemas oficiais da Rockstar Games.

## Objetivo

Demonstrar, em um ambiente local reproduzível, como Kafka absorve rajadas de eventos quando a produção supera a capacidade de consumo. O experimento mede throughput, consumer lag, latência de fila, recuperação de backlog e os limites de paralelismo impostos pelo número de partitions.

O cenário pode ser executado antes ou depois do lançamento do jogo: os experimentos independem do calendário oficial e não acompanham uma campanha real. Seu valor está na reprodução dos testes e na análise das medições locais.

## Arquitetura planejada

```text
Gerador de eventos sintéticos
          ↓
        Kafka
          ├── Consumer group: métricas de campanha → PostgreSQL
          └── Consumer group: usuários ativos → PostgreSQL
                    ↓
     Observabilidade: lag, throughput, latência e recursos locais
```

## Cenários de carga sintéticos

| Perfil | Taxa alvo |
| --- | ---: |
| Normal | 500 eventos/s |
| Campanha/trailer | 5.000 eventos/s |
| Pico viral | 20.000 eventos/s |

## Como iniciar a infraestrutura local

Com Docker e Docker Compose disponíveis, execute na raiz do repositório:

```bash
cp -n .env.example .env
docker compose up -d kafka postgres kafka-ui
docker compose ps
```

O `cp -n` cria o `.env` apenas se ele ainda não existir, sem sobrescrever sua configuração local. Esse arquivo não deve ser versionado. O `docker compose ps` deve mostrar os três serviços em execução. A interface Kafka UI fica em <http://localhost:8081>.

Para parar os serviços sem remover os volumes de dados:

```bash
docker compose down
```

## Próximas etapas

1. Subir Kafka, PostgreSQL e ferramentas de observabilidade com Docker Compose.
2. Criar o tópico de clickstream com seis partitions.
3. Implementar o gerador de eventos e os dois consumer groups.
4. Executar e documentar experimentos de formação e recuperação de backlog.

Consulte [o PRD](docs/prd.md), [a arquitetura](docs/architecture.md), [o contrato de eventos](docs/event-contract.md) e [o roadmap](docs/roadmap.md).

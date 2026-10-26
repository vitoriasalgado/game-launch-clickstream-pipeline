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

## Criar o tópico Kafka

Com o Kafka iniciado conforme a seção anterior, execute os comandos abaixo na raiz do repositório. O script usa caminhos relativos a essa pasta.

Use Python 3.14. Se ainda não tiver um ambiente virtual, crie-o para manter as dependências do projeto separadas das demais instalações Python:

```bash
python3 -m venv .venv
```

Ative o ambiente no terminal e instale as dependências, incluindo as ferramentas de desenvolvimento:

```bash
source .venv/bin/activate
python -m pip install ".[dev]"
```

Defina o endereço do Kafka para o programa executado no Mac:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Essa variável vale para a sessão atual do terminal. Em um novo terminal, defina-a novamente antes de executar o script. O programa não carrega o arquivo `.env` automaticamente.

No mesmo terminal, execute o script de administração:

```bash
python src/admin/create_topic.py
```

O script consulta o Kafka e lê o primeiro tópico definido em `config/topics.yaml`. Na configuração padrão, ele cria `game-clickstream-events` com seis partitions, fator de replicação 1 e retenção de 86.400.000 milissegundos (um dia).

- Se o tópico não existir, o script solicita a criação e aguarda a confirmação do Kafka antes de mostrar `Tópico criado com sucesso.`
- Se o tópico já existir, mostra `O tópico já existe.`

O script não atualiza nem compara as configurações de um tópico existente. Portanto, mudar partitions ou retenção no YAML e executar novamente não aplica essas mudanças ao Kafka. Confira as configurações no Kafka UI em <http://localhost:8081>.

## Próximas etapas

1. Subir Kafka, PostgreSQL e ferramentas de observabilidade com Docker Compose.
2. Criar o tópico de clickstream com seis partitions.
3. Implementar o gerador de eventos e os dois consumer groups.
4. Executar e documentar experimentos de formação e recuperação de backlog.

Consulte [o PRD](docs/prd.md), [a arquitetura](docs/architecture.md), [o contrato de eventos](docs/event-contract.md) e [o roadmap](docs/roadmap.md).

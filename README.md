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

## Publicar um evento de exemplo

Com o Kafka rodando, o tópico criado e o ambiente virtual ativo, execute na raiz do repositório. Defina o endereço do broker na mesma sessão do terminal:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Execute o programa de publicação:

```bash
python src/producer/publish_example.py
```

Cada execução gera um evento sintético `game_page_view` com novo UUID e horário UTC, converte os dados para JSON e publica uma mensagem no primeiro tópico de `config/topics.yaml`. A chave Kafka recebe o mesmo valor do campo `session_id`: `session_exemplo_01` neste exemplo. O contrato está em [docs/event-contract.md](docs/event-contract.md).

O programa mostra `Evento entregue ao Kafka.` quando o callback recebe a confirmação de sucesso. Depois, exibe a quantidade de mensagens ainda pendentes ao terminar a espera de até dez segundos. O valor `0` significa que não restaram mensagens pendentes; sozinho, ele não confirma sucesso, pois uma falha de entrega também pode concluir a operação. Confira a resposta do callback.

Para inspecionar a publicação, abra <http://localhost:8081>, selecione `game-clickstream-events` e entre em **Messages**. Confira a chave, o JSON, a partition e o offset. Execute o programa novamente e atualize a lista para comparar dois eventos da mesma sessão.

Na validação manual de 27/10/2026, foram observados dois eventos com UUIDs diferentes e a seguinte distribuição:

| Evento | Chave Kafka | Partition | Offset |
| --- | --- | ---: | ---: |
| Primeiro | `session_exemplo_01` | 0 | 0 |
| Segundo | `session_exemplo_01` | 0 | 1 |

Essa observação confirma que, nessa execução, a mesma chave encaminhou os dois eventos à mesma partition, com offsets crescentes. Os números acima são evidência desse teste local, não valores que toda execução deva reproduzir. Outras sessões podem ir para outras partitions; a escolha também depende da regra de particionamento e da quantidade de partitions.

## Verificações locais

Com o ambiente virtual ativo, os comandos abaixo verificam problemas no código, conferem a formatação e executam os testes, respectivamente:

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m pytest tests -v
```

Os testes automatizados atuais verificam a configuração de partitions e os campos do evento de exemplo, seu tipo e origem, o formato do primeiro UUID e a diferença entre dois identificadores, além do horário ISO 8601 com fuso UTC. A entrega no Kafka e a atribuição observada de partition foram validadas manualmente conforme o procedimento acima.

## Convenção de nomes de funções

As funções criadas no projeto devem ter nomes em português, em `snake_case` e sem acentos, como `criar_evento` e `confirmar_entrega`. Funções de teste preservam o prefixo `test_`, seguido de um nome em português. Essa convenção também se aplica a novas funções e à revisão gradual do código existente.

Nomes de funções e métodos das bibliotecas, campos do contrato de evento e chaves de configuração de APIs permanecem conforme suas definições originais.

## Próximas etapas

1. Implementar o gerador configurável com os seis tipos de evento e testes.
2. Implementar o consumo mínimo e os dois consumer groups.
3. Medir throughput, consumer lag, latência e recursos locais.
4. Executar e documentar experimentos de formação e recuperação de backlog.

Consulte [o PRD](docs/prd.md), [a arquitetura](docs/architecture.md), [o contrato de eventos](docs/event-contract.md) e [o roadmap](docs/roadmap.md).

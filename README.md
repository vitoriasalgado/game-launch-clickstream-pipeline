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

## Executar o gerador de carga sintética

Com o Kafka rodando, o tópico criado e o ambiente virtual ativo, execute na raiz do projeto. Defina o endereço do broker e escolha um dos perfis `normal`, `campaign_trailer` ou `viral_peak`:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
export LOAD_PROFILE=normal
python -m src.producer.gerar_eventos
```

`LOAD_PROFILE` seleciona a configuração em `config/load-profiles.yaml`; se não for definido, o padrão é `normal`. Cada perfil configura:

- `target_events_per_second`: taxa alvo de enfileiramento, não garantia de entrega.
- `duration_seconds`: janela em que o programa tenta gerar e enfileirar eventos.
- `active_sessions`: quantidade de sessões sintéticas reutilizadas durante a execução.
- `event_type_weights`: pesos relativos para sortear os seis tipos. Eles não representam conversões reais nem garantem proporções exatas em cada sessão.
- `delivery_timeout_ms`: prazo de entrega de uma mensagem, usado pelo cliente Kafka.
- `flush_timeout_seconds`: espera máxima pela conclusão dos envios ao terminar a janela de geração.

O gerador sorteia uma sessão e um tipo por evento, publica o JSON usando `session_id` como chave e processa as confirmações durante a execução. Se a fila local do cliente estiver cheia, espera espaço e tenta novamente o mesmo evento, respeitando a janela de geração. Não há impressão nem `flush` por mensagem: a drenagem ocorre uma vez ao final. A criação automática de tópicos está desabilitada, e a idempotência do producer está habilitada para preservar a ordenação nas retentativas do cliente. Isso não representa tolerância a falhas distribuída: o ambiente continua com um broker e fator de replicação 1.

Ao terminar, o programa imprime um resumo JSON com contagens e tempos medidos:

| Campo | Significado |
| --- | --- |
| `gerados` | Eventos construídos em memória. |
| `enfileirados` | Eventos aceitos na fila local do producer; não são confirmações de entrega. |
| `entregues` | Entregas confirmadas com sucesso pelo callback. |
| `falhas_entrega` | Envios concluídos com erro no callback. |
| `nao_enfileirados` | Eventos gerados que não entraram na fila antes do fim da janela. |
| `pendentes` | Mensagens ainda pendentes quando a espera final terminou. |
| `esperas_fila_cheia` | Tentativas adiadas por falta de espaço na fila local; não equivalem a consumer lag. |
| `primeiro_erro` | Primeira falha de entrega reportada, quando houver. |
| `tempo_geracao_s` | Duração observada da geração, incluindo controle de taxa e espera por espaço. |
| `tempo_drenagem_s` | Tempo gasto aguardando os envios após encerrar a geração. |
| `tempo_total_s` | Geração mais drenagem final. |
| `taxa_enfileiramento_observada_eventos_s` | Enfileirados divididos pelo tempo de geração. |
| `taxa_entrega_observada_eventos_s` | Entregues divididos pelo tempo total, incluindo a drenagem. |

A taxa alvo é apresentada separadamente das taxas observadas. Falhas, pendências ou eventos não enfileirados resultam em código de saída 1; uma taxa observada abaixo da meta não é, por si só, erro de execução. O processo pode durar mais que `duration_seconds` devido à drenagem final. Atingir 500, 5.000 ou 20.000 eventos/s deve ser verificado no MacBook e registrado com o resumo da execução; testes unitários não comprovam essas taxas reais.

O sorteio modela intensidade e mistura de eventos, não um funil de navegação causal: ele não exige que uma sessão passe por checkout antes de um evento de conclusão. Essa é uma simplificação explícita do experimento sintético de capacidade.

## Verificações locais

Com o ambiente virtual ativo, os comandos abaixo verificam problemas no código, conferem a formatação e executam os testes, respectivamente:

```bash
python -m ruff check src tests
python -m ruff format --check src tests
python -m pytest tests -v
```

Os testes automatizados verificam a configuração de partitions, o contrato do evento, os seis tipos permitidos, usuário e sessão, o formato do primeiro UUID e a diferença entre identificadores, além do horário ISO 8601 em UTC. Para o gerador de carga, um relógio e um producer simulados verificam controle de taxa, chave igual ao `session_id` do JSON, contagem de confirmações e falhas, fila cheia, drenagem limitada e cálculos de throughput. Esses testes não acessam um broker. A entrega do evento de exemplo e sua partition foram validadas manualmente conforme o procedimento acima; a carga real do gerador precisa de sua própria execução e registro.

## Convenção de nomes de funções

As funções criadas no projeto devem ter nomes em português, em `snake_case` e sem acentos, como `criar_evento` e `confirmar_entrega`. Funções de teste preservam o prefixo `test_`, seguido de um nome em português. Essa convenção também se aplica a novas funções e à revisão gradual do código existente.

Nomes de funções e métodos das bibliotecas, campos do contrato de evento e chaves de configuração de APIs permanecem conforme suas definições originais.

## Próximas etapas

1. Executar os perfis do gerador no ambiente local e registrar taxas efetivas e limitações.
2. Implementar o consumo mínimo e os dois consumer groups.
3. Medir throughput, consumer lag, latência e recursos locais.
4. Executar e documentar experimentos de formação e recuperação de backlog.

Consulte [o PRD](docs/prd.md), [a arquitetura](docs/architecture.md), [o contrato de eventos](docs/event-contract.md) e [o roadmap](docs/roadmap.md).

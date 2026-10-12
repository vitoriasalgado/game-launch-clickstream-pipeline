# PRD — Pipeline de Clickstream para Campanha Sintética

## 1. Contexto

Uma campanha promocional de um lançamento de grande alcance pode concentrar acessos em janelas curtas, como a publicação de trailers e a abertura de pré-venda. Este projeto simula esse cenário para estudar o comportamento de um pipeline de eventos sob pressão.

O caso não usa dados reais: eventos, volumes, campanhas e resultados são sintéticos. O objetivo é demonstrar engenharia de dados e planejamento de capacidade, não projetar demanda oficial de GTA VI ou da Rockstar Games.

GTA VI serve como inspiração para uma campanha fictícia de pré-venda. O estudo é reproduzível antes ou depois do lançamento do jogo, sem depender de sua data oficial ou da execução de uma campanha real. A validade dos resultados se limita às condições e aos recursos registrados em cada experimento local.

## 2. Problema

Quando produtores publicam mais eventos do que consumidores conseguem processar, um sistema precisa absorver a diferença sem perder eventos e permitir que o processamento se recupere depois. Também é preciso enxergar o backlog, o tempo em fila e o efeito de aumentar a capacidade de consumo.

## 3. Objetivo do produto

Construir um ambiente local e reproduzível que demonstre como Kafka funciona como buffer de eventos durante rajadas de clickstream, permitindo acompanhar e reduzir consumer lag por meio de escala horizontal limitada pelo número de partitions.

## 4. Perguntas que o projeto deve responder

1. O que acontece quando a taxa de produção supera a capacidade de um consumer group?
2. Como o consumer lag e a latência de processamento evoluem durante uma rajada?
3. Quanto tempo é necessário para recuperar um backlog após reduzir a carga ou aumentar a capacidade de consumo?
4. Até que ponto adicionar consumers aumenta throughput em um group?
5. Como o número de partitions limita o paralelismo máximo de cada consumer group?

## 5. Escopo do MVP

### Incluído

- Gerador configurável de eventos de clickstream sintéticos.
- Tópico Kafka com múltiplas partitions e chave `session_id`.
- Dois consumer groups independentes, lendo o mesmo tópico.
- Persistência de agregados e sessões ativas no PostgreSQL.
- Perfis de carga normal, campanha/trailer e pico viral.
- Observação de throughput, consumer lag, latência de fila e recursos locais.
- Experimentos documentados de backlog induzido e escala de consumers.
- Testes automatizados de contrato e uma verificação contínua de lint e testes.

### Fora do escopo

- Dados reais, integrações externas ou previsões de mercado.
- Cloud, Kubernetes, múltiplos brokers ou replicação distribuída.
- Kafka Connect, Schema Registry, Spark, Flink e machine learning.
- Autenticação, autorização e hardening de produção.
- Data lake em MinIO/Parquet no MVP; essa é uma extensão posterior.

## 6. Usuários e público

- Avaliadores de portfólio de engenharia de dados.
- Pessoas interessadas em sistemas de streaming e capacity planning.
- A própria autora, como ambiente prático de aprendizado e experimentação.

## 7. Fluxo funcional

1. O gerador cria eventos de clickstream com taxa configurável.
2. Cada evento é publicado no tópico `game-clickstream-events`, usando `session_id` como chave.
3. O grupo `campaign-metrics` calcula métricas por tipo de evento, página, campanha e janela de tempo.
4. O grupo `active-users` calcula sessões ou usuários ativos em uma janela recente.
5. Ambos gravam seus resultados no PostgreSQL de forma independente.
6. As ferramentas de observabilidade mostram taxa, lag, latência e recursos durante os testes.

## 8. Requisitos funcionais

| ID | Requisito |
| --- | --- |
| RF-01 | O gerador deve produzir os seis tipos de evento definidos no contrato. |
| RF-02 | A taxa de eventos deve ser configurável por perfil de carga. |
| RF-03 | Eventos da mesma sessão devem usar a mesma chave de partição. |
| RF-04 | O tópico deve ter número de partitions configurável, com seis como padrão. |
| RF-05 | O grupo de métricas deve agregar eventos por janela de tempo e dimensões de campanha. |
| RF-06 | O grupo de usuários ativos deve acompanhar atividade recente de forma independente. |
| RF-07 | Os resultados dos dois grupos devem ser persistidos no PostgreSQL. |
| RF-08 | O projeto deve expor ou registrar throughput, lag e latência. |
| RF-09 | Deve existir um roteiro reproduzível para induzir e recuperar backlog. |
| RF-10 | O gerador deve ter teste automatizado que valide o contrato, os seis tipos de evento e o uso de `session_id` como chave. |

## 9. Requisitos não funcionais

| ID | Requisito |
| --- | --- |
| RNF-01 | O ambiente deve iniciar em um MacBook via Docker Compose. |
| RNF-02 | Configurações e perfis de carga devem ficar versionados. |
| RNF-03 | O projeto deve declarar claramente que todos os dados são sintéticos. |
| RNF-04 | A execução de um experimento deve ser documentada e reproduzível. |
| RNF-05 | O MVP deve permanecer simples o bastante para uso local. |
| RNF-06 | Todo pull request ou push para a branch principal deve executar lint e testes automatizados em CI. |

## 10. Eventos

Os eventos permitidos são `game_page_view`, `trailer_play`, `wishlist_added`, `edition_compared`, `preorder_checkout_started` e `preorder_completed`. O contrato completo está em [event-contract.md](event-contract.md).

## 11. Cenários de carga sintéticos

| Cenário | Taxa alvo | Uso no experimento |
| --- | ---: | --- |
| Normal | 500 eventos/s | Linha de base estável. |
| Campanha/trailer | 5.000 eventos/s | Teste de pressão moderada. |
| Pico viral | 20.000 eventos/s | Formação deliberada de backlog. |

As taxas são metas de geração, não garantias de throughput do computador local.

## 12. Métricas de sucesso

- É possível observar crescimento de lag quando produção excede consumo.
- Os dois consumer groups leem integralmente o tópico de forma independente.
- A escala de consumers reduz o backlog enquanto houver partitions disponíveis.
- Consumers acima do número de partitions não aumentam o paralelismo do group.
- Pelo menos um experimento completo é reproduzível e documentado no repositório.
- O contrato do gerador é validado automaticamente antes de integração na branch principal.

## 13. Hipóteses experimentais

1. Quando a produção supera o consumo, o consumer lag cresce e Kafka retém o backlog.
2. Aumentar o número de consumers até o número de partitions aumenta a capacidade do consumer group.
3. Consumers acima do número de partitions ficam ociosos ou não aumentam o throughput do mesmo group.
4. O backlog só é recuperado quando a capacidade de consumo supera a taxa de produção.

## 14. Riscos e decisões

| Risco | Mitigação |
| --- | --- |
| 20.000 eventos/s exceder a capacidade local | Tratar taxa como configurável e registrar a taxa efetiva observada. |
| Observabilidade adicionar complexidade | Priorizar Kafka UI e métricas essenciais antes de dashboards elaborados. |
| Banco se tornar o gargalo | Usar agregações por lote e separar a medição do pipeline da persistência. |
| Escopo crescer demais | Manter MinIO, Parquet e cloud como extensões pós-MVP. |
| Regressão no contrato de eventos | Cobrir contrato com testes unitários e executá-los no CI. |

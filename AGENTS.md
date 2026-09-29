# AGENTS.md

## Propósito do repositório

Este projeto é um pipeline de clickstream **inteiramente sintético** para experimentos locais de Kafka. Ele demonstra formação e recuperação de backlog, consumer lag, throughput, latência de fila e o limite de paralelismo imposto por partitions.

Não represente eventos, taxas de carga, métricas ou conclusões como dados reais, previsões ou sistemas da Rockstar Games/GTA VI.

## Prioridade de entrega

Construa e valide o MVP nesta ordem:

1. Infraestrutura local: Kafka em KRaft, PostgreSQL e Kafka UI com Docker Compose.
2. Criação do tópico `game-clickstream-events` com seis partitions.
3. Gerador de eventos sintéticos configurável.
4. Consumer group mínimo e observação de offsets/lag.
5. Consumer group `campaign-metrics` com agregações no PostgreSQL.
6. Consumer group `active-users` com sessões ativas no PostgreSQL.
7. Métricas de throughput, lag, latência e recursos locais.
8. Experimentos reproduzíveis de backlog, escala e limite de partitions.

Não introduza MinIO, Parquet, cloud, Kubernetes, Spark, Flink, Kafka Connect ou Schema Registry no MVP, salvo solicitação explícita. São extensões pós-MVP.

## Decisões arquiteturais obrigatórias

- Kafka é o buffer entre produtores e consumidores; não o substitua por filas internas ou persistência direta no banco.
- O tópico principal é `game-clickstream-events`.
- O padrão é seis partitions; toda alteração deve atualizar configuração, documentação e experimentos.
- Use `session_id` como chave Kafka. Isso preserva ordem **dentro da sessão**, mas não entre sessões distintas do mesmo `user_id`.
- Há dois consumer groups independentes:
  - `campaign-metrics`: agregações por janela, campanha, página e tipo de evento.
  - `active-users`: atividade recente por sessão ou usuário.
- Os dois grupos devem ler o tópico completo independentemente. Não os combine em um único group.
- PostgreSQL é o armazenamento do MVP para agregados e estado operacional.
- Toda configuração de carga deve declarar que é sintética e ficar fora do código, em `config/load-profiles.yaml`.

## Contrato de evento

O contrato-fonte está em `docs/event-contract.md`. Todo evento deve conter, no mínimo:

- `event_id` único;
- `event_type` válido;
- `occurred_at` em UTC e ISO 8601;
- `user_id`;
- `session_id`;
- `campaign_id`;
- `source: synthetic-generator`.

Tipos permitidos:

- `game_page_view`
- `trailer_play`
- `wishlist_added`
- `edition_compared`
- `preorder_checkout_started`
- `preorder_completed`

Não altere o contrato silenciosamente. Qualquer campo obrigatório novo, remoção ou mudança semântica exige atualização simultânea do contrato, dos testes e dos consumidores afetados.

## Qualidade e validação

- Escreva testes unitários com `pytest` para código novo, começando pelo contrato e pelo gerador.
- O teste mínimo do gerador deve validar todos os seis tipos de evento, campos obrigatórios, timestamps válidos e uso de `session_id` como chave de publicação.
- Use Ruff para lint e formatação após a configuração do projeto Python.
- Configure GitHub Actions para executar lint e testes em push e pull request assim que `pyproject.toml` existir.
- Não afirme que uma taxa alvo foi alcançada sem registrar a taxa efetiva observada durante o experimento.
- Antes de concluir uma alteração, execute os testes e verificações disponíveis; informe claramente o que não pôde ser validado.

## Experimentos e observabilidade

Cada experimento em `docs/experiments/` deve registrar:

1. perfil de carga e taxa efetiva de produção;
2. número de partitions;
3. quantidade de consumers em cada group;
4. duração do teste;
5. throughput de produção e consumo;
6. lag por group e, quando possível, por partition;
7. latência de processamento/fila;
8. tempo de recuperação do backlog;
9. CPU e memória dos containers;
10. conclusão e limitações locais.

Para provar o limite de paralelismo, compare ao menos três condições no mesmo consumer group: menos consumers que partitions, número igual de consumers e mais consumers que partitions.

## Convenções de implementação

- Código compartilhado: `src/common/`.
- Gerador: `src/producer/`.
- Consumidores: `src/consumers/campaign_metrics/` e `src/consumers/active_users/`.
- Administração Kafka: `src/admin/`.
- Esquema e consultas PostgreSQL: `sql/`.
- Testes unitários e de integração: `tests/unit/` e `tests/integration/`.
- Dashboards e configuração de observabilidade: `observability/`.
- Scripts operacionais reproduzíveis: `scripts/`.

Prefira funções pequenas, tipadas e testáveis. Mantenha configurações em arquivos ou variáveis de ambiente; não embuta endereços, credenciais ou taxas de carga no código. Nunca versione arquivos `.env`.

## Documentação obrigatória

Mantenha consistentes os documentos abaixo quando a mudança os afetar:

- `README.md`: instruções de execução e visão do projeto;
- `docs/prd.md`: escopo e requisitos;
- `docs/architecture.md`: decisões arquiteturais e trade-offs;
- `docs/event-contract.md`: formato e regras do evento;
- `docs/roadmap.md`: marcos e critérios de aceite.

## Segurança de alterações

- **Não implemente código, infraestrutura, automações ou qualquer mudança de arquivo sem aprovação explícita da usuária.** Em especial, não crie, altere ou execute implementações em `src/`, `tests/`, `scripts/`, `sql/`, Docker Compose ou workflows de CI sem essa autorização.
- Quando a solicitação for de análise, planejamento, revisão ou explicação, limite-se a ações de leitura e à resposta solicitada. Proponha mudanças, mas aguarde aprovação antes de aplicá-las.
- **Modo de aprendizagem de Python:** a usuária está aprendendo Python. Não escreva, complete ou implemente código por ela, mesmo quando houver aprovação para avançar uma etapa. Atue como mentora: explique conceitos, proponha passos pequenos, faça perguntas de raciocínio quando úteis, forneça pseudocódigo ou estrutura conceitual somente se solicitado e revise o código que ela escrever. Ao sugerir mudanças, descreva o objetivo e os critérios de validação para que ela mesma implemente.
- Preserve trabalho existente e não reverta alterações não relacionadas.
- Não inclua segredos, dados pessoais reais ou dados de terceiros.
- Não use comandos destrutivos para recriar volumes ou bancos sem confirmação explícita.
- Prefira mudanças pequenas, verificáveis e alinhadas ao marco atual do roadmap.

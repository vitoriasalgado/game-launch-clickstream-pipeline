# Roadmap incremental

Cada etapa deve terminar com uma evidência verificável — comando, teste, captura de tela, consulta SQL ou documento de experimento. Não avance para a próxima etapa se a anterior não puder ser reproduzida.

## Marco 0 — Fundação do repositório

**Entrega:** documentação inicial, estrutura de diretórios, contrato de evento e perfis de carga sintéticos.

**Critérios de aceite:**

- README informa claramente o caráter sintético do projeto.
- PRD, arquitetura, contrato de evento e plano de capacidade estão versionados.
- Os perfis de 500, 5.000 e 20.000 eventos/s estão centralizados em configuração.
- A estratégia de qualidade está definida: `pytest` para testes e Ruff para lint, executados por GitHub Actions.

## Marco 1 — Infraestrutura local

**Entrega:** Docker Compose inicia Kafka em KRaft, PostgreSQL e Kafka UI.

**Critérios de aceite:**

- Os serviços sobem com um único comando documentado.
- Kafka UI lista o cluster local.
- PostgreSQL aceita uma conexão local.
- A execução cabe no ambiente alvo, um MacBook com containers locais.
- O CI executa lint e a suíte de testes em cada push e pull request.

**Dependência:** Marco 0.

## Marco 2 — Tópico e contrato de eventos

**Entrega:** criação automatizada de `game-clickstream-events` com seis partitions.

**Critérios de aceite:**

- O tópico possui seis partitions e retenção configurada.
- Um evento JSON de exemplo respeita o contrato.
- A chave de publicação é `session_id`.
- Kafka UI permite inspecionar mensagens e distribuição entre partitions.

**Dependência:** Marco 1.

## Marco 3 — Produtor sintético

**Entrega:** gerador com perfis de carga e distribuição configurável de tipos de evento.

**Critérios de aceite:**

- O gerador publica os seis tipos permitidos.
- A taxa é configurável sem alterar o código.
- A taxa efetiva de publicação é registrada.
- Uma mesma sessão sempre é enviada com a mesma chave.
- Testes unitários validam evento, tipos permitidos e chave `session_id`.

**Dependência:** Marco 2.

## Marco 4 — Consumo mínimo viável

**Entrega:** consumer group de teste que processa eventos e confirma offsets.

**Critérios de aceite:**

- O group reinicia sem perder sua posição de consumo.
- O lag aparece no Kafka UI quando o consumer é interrompido.
- Retomar o consumer reduz o lag quando seu throughput excede a produção.

**Dependência:** Marco 3.

## Marco 5 — Consumer group de métricas

**Entrega:** `campaign-metrics` grava agregações no PostgreSQL.

**Critérios de aceite:**

- As agregações incluem janela de tempo, tipo de evento, página e campanha.
- Consultas SQL mostram métricas consistentes com os eventos publicados.
- O consumer processa em lotes e tem estratégia documentada para reprocessamento.

**Dependência:** Marco 4.

## Marco 6 — Consumer group de usuários ativos

**Entrega:** `active-users` acompanha sessões ou usuários ativos recentemente.

**Critérios de aceite:**

- O group tem identificador diferente de `campaign-metrics`.
- Ambos leem o mesmo tópico sem disputar mensagens entre si.
- Uma consulta mostra sessões ou usuários ativos na janela configurada.

**Dependência:** Marco 5.

## Marco 7 — Observabilidade essencial

**Entrega:** visualização de lag, throughput, latência e recursos locais.

**Critérios de aceite:**

- O lag por group e partition é visível.
- Taxas de produção e consumo são registradas ou exibidas.
- Latência entre geração e processamento é mensurável.
- CPU e memória dos containers podem ser observadas durante uma carga.

**Dependência:** Marcos 3 a 6.

## Marco 8 — Experimento de backlog

**Entrega:** cenário reproduzível no qual a produção supera o consumo.

**Critérios de aceite:**

- O cenário informa perfil de carga, número de partitions e consumers por group.
- O lag cresce de forma mensurável.
- A taxa de produção e a capacidade de consumo são registradas.
- A evidência fica em `docs/experiments/`.

**Dependência:** Marco 7.

## Marco 9 — Experimento de escala e limite de partitions

**Entrega:** comparação entre diferentes quantidades de consumers no mesmo group.

**Critérios de aceite:**

- Há ao menos uma execução abaixo, igual e acima do número de partitions.
- A comparação demonstra ganho até o limite das seis partitions.
- Consumers excedentes ficam ociosos ou não elevam o throughput do group.
- O tempo de recuperação do backlog é comparado entre as execuções.

**Dependência:** Marco 8.

## Marco 10 — Relatório final do MVP

**Entrega:** documentação final de resultados e limitações.

**Critérios de aceite:**

- Cada experimento contém configuração, métricas, evidências e conclusão.
- O README indica como reproduzir o cenário principal.
- As limitações do ambiente local estão explícitas.
- O projeto responde às cinco perguntas definidas no PRD.

**Dependência:** Marco 9.

## Pós-MVP opcional

- Persistir eventos brutos em Parquet no MinIO.
- Adicionar validação de esquema e evolução de contrato.
- Comparar diferentes números de partitions.
- Automatizar experimentos e consolidar gráficos.
- Migrar o ambiente para cloud ou Kubernetes somente como extensão deliberada.

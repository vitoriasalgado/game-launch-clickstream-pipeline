# Roadmap incremental

Cada etapa deve terminar com uma evidência verificável — comando, teste, captura de tela, consulta SQL ou documento de experimento. Não avance para a próxima etapa se a anterior não puder ser reproduzida.

## Pontos de commit

Faça um commit ao terminar uma entrega pequena, coerente e verificável. Um marco pode ter vários commits; não é preciso esperar seu encerramento. Os pontos abaixo são sugestões de divisão, não entregas já concluídas.

Antes de cada commit:

1. Explique a mudança em uma frase e confira os arquivos envolvidos.
2. Execute as verificações locais pertinentes. Mantenha código, testes e documentação afetada juntos quando fizerem parte da mesma mudança.
3. Selecione apenas os arquivos da entrega e revise o conteúdo preparado para commit. Exclua credenciais e arquivos gerados.
4. Use Conventional Commits: `feat` para funcionalidades, `fix` para correções, `test` para testes, `ci` para automações, `docs` para documentação e `chore` para manutenção/configuração.

As mensagens abaixo são exemplos; ajuste-as ao que foi realmente entregue. O commit é executado pela usuária, com orientação. Não crie commits vazios nem reescreva o histórico para encaixá-lo nessas sugestões.

Para CI, faça o commit após as verificações locais e confirme o resultado no GitHub após o push. Criar o workflow não basta para declarar o CI validado; se ele falhar, corrija a causa e registre a correção em outro commit.

## Marco 0 — Fundação do repositório

**Entrega:** documentação inicial, estrutura de diretórios, contrato de evento e perfis de carga sintéticos.

**Critérios de aceite:**

- README informa claramente o caráter sintético do projeto.
- PRD, arquitetura, contrato de evento e plano de capacidade estão versionados.
- Os perfis de 500, 5.000 e 20.000 eventos/s estão centralizados em configuração.
- A estratégia de qualidade está definida: `pytest` para testes e Ruff para lint, executados por GitHub Actions.

**Ponto de commit:** documentação e estrutura inicial revisadas — `chore: scaffold project structure`.

## Marco 1 — Infraestrutura local

**Entrega:** Docker Compose inicia Kafka em KRaft, PostgreSQL e Kafka UI.

**Critérios de aceite:**

- Os serviços sobem com um único comando documentado.
- Kafka UI lista o cluster local.
- PostgreSQL aceita uma conexão local.
- A execução cabe no ambiente alvo, um MacBook com containers locais.
- O CI executa lint e a suíte de testes em cada push e pull request.

**Dependência:** Marco 0.

**Pontos de commit:**

- Serviços locais funcionando e configuração Python validada — `chore: configure local infrastructure and Python tooling`.
- Instruções de execução conferidas — `docs: document local infrastructure setup`.
- Primeiro teste útil escrito e passando, com Ruff sem erros — `test: add initial project validation`.
- Workflow de lint e testes configurado, com os comandos conferidos localmente — `ci: run lint and tests on push and pull request`. Após o push, confirme a execução no GitHub para fechar o marco.

## Marco 2 — Tópico e contrato de eventos

**Entrega:** criação automatizada de `game-clickstream-events` com seis partitions.

**Critérios de aceite:**

- O tópico possui seis partitions e retenção configurada.
- Um evento JSON de exemplo respeita o contrato.
- A chave de publicação é `session_id`.
- Kafka UI permite inspecionar mensagens e distribuição entre partitions.

**Dependência:** Marco 1.

**Pontos de commit:**

- Criação automatizada do tópico validada, incluindo nova execução quando ele já existe — `feat: automate clickstream topic creation`.
- Evento de exemplo validado e publicação com chave `session_id` inspecionada no Kafka UI — `test: verify event contract and session key`.

## Marco 3 — Produtor sintético

**Entrega:** gerador com perfis de carga e distribuição configurável de tipos de evento.

**Critérios de aceite:**

- O gerador publica os seis tipos permitidos.
- A taxa é configurável sem alterar o código.
- A taxa efetiva de publicação é registrada.
- Uma mesma sessão sempre é enviada com a mesma chave.
- Testes unitários validam evento, tipos permitidos e chave `session_id`.

**Dependência:** Marco 2.

**Pontos de commit:**

- Geração dos seis tipos de evento coberta por testes — `feat: generate synthetic clickstream events`.
- Publicação no Kafka com `session_id` como chave, com testes e inspeção de mensagens — `feat: publish events with session keys`.
- Perfis de carga aplicados e taxa efetiva medida — `feat: control load profiles and measure production rate`.

## Marco 4 — Consumo mínimo viável

**Entrega:** consumer group de teste que processa eventos e confirma offsets.

**Critérios de aceite:**

- O group reinicia sem perder sua posição de consumo.
- O lag aparece no Kafka UI quando o consumer é interrompido.
- Retomar o consumer reduz o lag quando seu throughput excede a produção.

**Dependência:** Marco 3.

**Pontos de commit:**

- Consumer processando eventos e retomando dos offsets confirmados após reiniciar — `feat: add minimal consumer with offset recovery`.
- Procedimento de interrupção e retomada com evidência de lag — `docs: record consumer pause and recovery validation`.

## Marco 5 — Consumer group de métricas

**Entrega:** `campaign-metrics` grava agregações no PostgreSQL.

**Critérios de aceite:**

- As agregações incluem janela de tempo, tipo de evento, página e campanha.
- Consultas SQL mostram métricas consistentes com os eventos publicados.
- O consumer processa em lotes e tem estratégia documentada para reprocessamento.

**Dependência:** Marco 4.

**Pontos de commit:**

- Agregações por janela testadas e persistência no PostgreSQL validada com consultas — `feat: persist campaign metrics aggregates`.
- Processamento em lotes e comportamento de reprocessamento implementados, testados e documentados — `feat: handle campaign metrics batch reprocessing`.

## Marco 6 — Consumer group de usuários ativos

**Entrega:** `active-users` acompanha sessões ou usuários ativos recentemente.

**Critérios de aceite:**

- O group tem identificador diferente de `campaign-metrics`.
- Ambos leem o mesmo tópico sem disputar mensagens entre si.
- Uma consulta mostra sessões ou usuários ativos na janela configurada.

**Dependência:** Marco 5.

**Pontos de commit:**

- Grupo de usuários ativos com estado no PostgreSQL e janela de atividade validada — `feat: track active sessions in PostgreSQL`.
- Evidência de que os dois grupos leem o tópico independentemente — `docs: verify independent consumer groups`.

## Marco 7 — Observabilidade essencial

**Entrega:** visualização de lag, throughput, latência e recursos locais.

**Critérios de aceite:**

- O lag por group e partition é visível.
- Taxas de produção e consumo são registradas ou exibidas.
- Latência entre geração e processamento é mensurável.
- CPU e memória dos containers podem ser observadas durante uma carga.

**Dependência:** Marcos 3 a 6.

**Pontos de commit:**

- Throughput e latência calculados e conferidos com eventos de teste — `feat: measure throughput and processing latency`.
- Acompanhamento de lag por grupo/partition e recursos dos containers validado — `feat: expose lag and local resource monitoring`.

## Marco 8 — Experimento de backlog

**Entrega:** cenário reproduzível no qual a produção supera o consumo.

**Critérios de aceite:**

- O cenário informa perfil de carga, número de partitions e consumers por group.
- O lag cresce de forma mensurável.
- A taxa de produção e a capacidade de consumo são registradas.
- A evidência fica em `docs/experiments/`.

**Dependência:** Marco 7.

**Pontos de commit:**

- Configuração e procedimento do experimento executáveis e conferidos — `chore: prepare reproducible backlog experiment`.
- Resultados, taxas efetivas e limitações registrados após a execução — `docs: record backlog experiment results`.

## Marco 9 — Experimento de escala e limite de partitions

**Entrega:** comparação entre diferentes quantidades de consumers no mesmo group.

**Critérios de aceite:**

- Há ao menos uma execução abaixo, igual e acima do número de partitions.
- A comparação demonstra ganho até o limite das seis partitions.
- Consumers excedentes ficam ociosos ou não elevam o throughput do group.
- O tempo de recuperação do backlog é comparado entre as execuções.

**Dependência:** Marco 8.

**Pontos de commit:**

- Cada condição executada e documentada pode gerar um commit próprio — por exemplo, `docs: record experiment with three consumers`.
- Comparação concluída entre menos, igual e mais consumers que partitions, incluindo recuperação do backlog — `docs: compare consumer scaling and partition limits`.

## Marco 10 — Relatório final do MVP

**Entrega:** documentação final de resultados e limitações.

**Critérios de aceite:**

- Cada experimento contém configuração, métricas, evidências e conclusão.
- O README indica como reproduzir o cenário principal.
- As limitações do ambiente local estão explícitas.
- O projeto responde às cinco perguntas definidas no PRD.

**Dependência:** Marco 9.

**Pontos de commit:**

- Relatório respondendo às cinco perguntas do PRD com evidências — `docs: summarize MVP capacity planning results`.
- README revisado após conferir a reprodução do cenário principal — `docs: finalize MVP reproduction guide`.

## Pós-MVP opcional

- Persistir eventos brutos em Parquet no MinIO.
- Adicionar validação de esquema e evolução de contrato.
- Comparar diferentes números de partitions.
- Automatizar experimentos e consolidar gráficos.
- Migrar o ambiente para cloud ou Kubernetes somente como extensão deliberada.

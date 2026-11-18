# Validação do consumer mínimo — pausa e retomada

## Contexto e objetivo

Validação local em **18/11/2026**, com eventos inteiramente sintéticos. O objetivo foi observar a retomada do group `minimal-consumer` a partir de um offset confirmado e a recuperação de uma mensagem publicada enquanto o consumer estava parado.

Os resultados vieram de consultas de offsets ao broker, saídas de terminal fornecidas pela autora e observações no Kafka UI durante a execução. O lag 1 durante a pausa foi informado pela autora; a captura final do Kafka UI mostrou offset confirmado 2073, end offset 2073 e lag 0. As saídas do produtor e do consumer permitiram comparar o mesmo `event_id`.

Este registro não representa tráfego real, previsão de demanda ou sistemas da Rockstar Games/GTA VI.

## Configuração e medições disponíveis

| Parâmetro ou métrica | Condição ou resultado |
| --- | --- |
| Ambiente | MacBook com Kafka e Kafka UI em containers locais; recursos de hardware não registrados |
| Carga sintética | Publicação avulsa por `src.producer.publish_example`; sem perfil de carga contínua |
| Taxa efetiva de produção | Não medida; houve uma entrega confirmada na rodada de pausa e retomada |
| Tópico | `game-clickstream-events` |
| Partitions | 6 |
| Fator de replicação configurado | 1 |
| Chave do evento de exemplo | `session_exemplo_01`, valor de `session_id` |
| Consumer group | `minimal-consumer` |
| Consumers nesse group | 1 durante o consumo; 0 durante a pausa |
| Outros consumer groups | Quantidades não inspecionadas nesta validação |
| Processamento | Impressão de partition, offset e conteúdo no terminal |
| Confirmação | Commit manual síncrono após imprimir cada mensagem |
| Duração do teste | Não medida |
| Throughput de produção e consumo | Não medido em eventos/s |
| Lag observado na partition 0 | 0 → 1 → 0 |
| Latência de processamento/fila | Não medida |
| Tempo de recuperação | Não medido; observada a recuperação de uma mensagem |
| CPU e memória dos containers | Não coletadas |

## Procedimento de reprodução

Execute os comandos na raiz do repositório, com Kafka e Kafka UI disponíveis, o tópico criado e as dependências instaladas em `.venv`. Mantenha apenas um consumer ativo no group `minimal-consumer` e suspenda outras publicações no tópico para isolar esta rodada.

### 1. Iniciar o consumer

No terminal 1, defina o endereço do broker. Essa variável vale para a sessão desse terminal; os programas não carregam `.env` automaticamente:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Inicie o consumer e deixe-o em execução. Ele pode permanecer silencioso se não houver mensagens disponíveis:

```bash
.venv/bin/python -m src.consumers.consumir_eventos
```

### 2. Estabelecer uma posição confirmada

Se o group ainda não tiver offsets confirmados, publique um evento de preparação com o consumer ativo. No terminal 2, configure também o endereço do Kafka:

```bash
export KAFKA_BOOTSTRAP_SERVERS=localhost:9092
```

Execute o produtor de exemplo para enviar uma mensagem sintética:

```bash
.venv/bin/python -m src.producer.publish_example
```

Confira `Evento entregue ao Kafka.` e a quantidade final de mensagens pendentes. O valor `0` sozinho não comprova sucesso: também é necessário conferir o callback de entrega. No terminal 1, confira a impressão da mensagem. No Kafka UI, abra o group `minimal-consumer`, atualize a página e confirme que a partition usada tem um offset confirmado igual ao end offset, com lag 0.

Registre essa partition e esses valores como referência da nova execução. Sem uma posição confirmada, o lag zero da interface não é uma referência suficiente para este teste.

### 3. Interromper e publicar

No terminal 1, pressione Ctrl+C. Aguarde `Consumo interrompido.` e o retorno do prompt. O group deve ficar sem membros ativos; o estado `Empty` é esperado nesse período.

No terminal 2, execute o mesmo comando do produtor de exemplo uma única vez, agora com o consumer parado. Registre o novo `event_id` e confira a entrega. Atualize o Kafka UI antes de reiniciar o consumer: o offset confirmado deve permanecer igual, enquanto o end offset da partition de destino avança e o lag passa a 1.

### 4. Retomar e conferir

No terminal 1, execute novamente o comando do consumer. Compare o `event_id` impresso com o da publicação feita durante a pausa e registre partition e offset. Deixe o consumer rodando e atualize o Kafka UI: o offset confirmado deve avançar para a posição seguinte à mensagem processada, alcançando o end offset, com lag 0.

Os offsets concretos variam entre execuções. Não redefina offsets, recrie o tópico ou remova volumes para tentar reproduzir os números deste registro.

## Evidências observadas

A preparação consumiu o evento `ac7d976c-0ac1-4609-9668-56578f5f66cd` na partition 0, offset 2071. Uma consulta ao broker confirmou `CURRENT-OFFSET = 2072`, `LOG-END-OFFSET = 2072` e `LAG = 0` antes da nova pausa.

Depois de interromper o consumer, o produtor publicou o evento abaixo e informou `Evento entregue ao Kafka.`, seguido de `0` mensagens pendentes:

| Campo | Valor |
| --- | --- |
| `event_id` | `ce82b127-99d4-4ed6-9684-43658a3a21ce` |
| `event_type` | `game_page_view` |
| `occurred_at` | `2026-11-18T18:53:45.005872+00:00` |
| `session_id` | `session_exemplo_01` |
| `source` | `synthetic-generator` |

Na retomada, o terminal do consumer mostrou esse mesmo evento na partition 0, offset 2072. O Kafka UI mostrou o group `STABLE`, um membro, seis partitions atribuídas e os valores finais abaixo:

| Etapa na partition 0 | Offset confirmado | End offset | Lag |
| --- | ---: | ---: | ---: |
| Antes da nova publicação | 2072 | 2072 | 0 |
| Após publicar com o consumer parado | 2072 | 2073 | 1 |
| Após reiniciar e consumir | 2073 | 2073 | 0 |

O commit 2073 representa a próxima posição de leitura após processar a mensagem 2072. Não significa que uma mensagem de offset 2073 tenha sido recebida. As demais partitions não tiveram consumo demonstrado nesta rodada; a atribuição de seis partitions não comprova que houve mensagens processadas em todas elas.

## Conclusão e limitações

A rodada demonstrou retomada do mesmo group a partir da posição confirmada, formação de lag durante uma pausa e recuperação de um evento sintético após reiniciar. A publicação terminou antes da retomada, portanto a produção estava parada durante a recuperação; não houve comparação de taxas sob carga contínua.

- Foi usada interrupção controlada por Ctrl+C, não encerramento abrupto, falha do broker ou falha de commit.
- O processamento foi a impressão no terminal, sem agregação ou persistência no PostgreSQL.
- Não foram medidos duração, throughput, latência, tempo de recuperação, CPU ou memória. O timestamp do evento não é uma medição dessas métricas.
- A recuperação de uma mensagem não determina capacidade máxima, estabilidade prolongada ou tolerância a falhas distribuída.
- Uma falha após processar e antes de confirmar pode causar releitura. Este teste não demonstra processamento exatamente uma vez.
- O teste não compara múltiplos consumers nem o limite de paralelismo das seis partitions.

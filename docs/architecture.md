# Arquitetura

O gerador publica eventos sintéticos no tópico `game-clickstream-events`, usando `session_id` como chave de partição. Isso preserva a ordem dos eventos dentro da mesma sessão.

### Decisão de particionamento

`session_id` é uma decisão intencional do MVP, e não uma simplificação acidental. O pipeline mede comportamento de navegação e atividade recente, ambos conceitos naturalmente organizados por sessão. A consequência aceita é que sessões diferentes do mesmo `user_id` podem cair em partitions distintas; portanto, não há garantia de ordem entre sessões de um mesmo usuário. Se uma extensão futura exigir uma linha do tempo global por usuário, a chave poderá ser alterada para `user_id`, com o trade-off de concentrar mais tráfego de usuários muito ativos na mesma partition.

Dois consumer groups leem o mesmo tópico de forma independente:

- `campaign-metrics`: produz agregações por página, campanha, tipo de evento e janela de tempo.
- `active-users`: acompanha sessões ou usuários ativos em janelas recentes.

Dentro de um consumer group, cada partition é atribuída a apenas um consumer por vez, mas um consumer pode receber várias partitions. Por exemplo, seis partitions podem ser distribuídas entre três consumers, com duas para cada um em uma atribuição equilibrada. Outro group recebe sua própria atribuição e lê essas mesmas partitions de forma independente.

Com seis partitions nesse tópico, no máximo seis consumers do mesmo group podem ter partitions atribuídas simultaneamente. Ao adicionar um sétimo, pelo menos um fica sem partition desse tópico; o limite de paralelismo do group não aumenta. Aumentar consumers até esse limite também não garante ganho proporcional de throughput: os experimentos devem medir o efeito da distribuição dos eventos e dos gargalos locais.

PostgreSQL guarda agregados e resultados operacionais dos experimentos. Kafka UI, Prometheus, Grafana e cAdvisor serão usados para observar o estado do pipeline e dos containers.

### Geração de carga e confirmação de entrega

O gerador lê taxa alvo, duração, quantidade de sessões, pesos dos tipos e prazos de entrega de `config/load-profiles.yaml`. Usa um relógio monotônico para planejar os envios a partir do início da execução e tenta acompanhar a taxa alvo sem garanti-la. Publica com `session_id` como chave e mantém a idempotência do producer habilitada para preservar a ordenação em retentativas. Se a fila local do cliente encher, processa confirmações e aguarda espaço até o fim da janela. Essa fila pertence ao cliente Kafka e não substitui o broker como buffer do pipeline.

As contagens distinguem eventos gerados, enfileirados, entregas confirmadas, falhas e pendências. O `flush` final tem prazo limitado; a taxa de entrega observada usa o total confirmado dividido pelo tempo de geração mais drenagem, enquanto a taxa de enfileiramento usa apenas a janela de geração. Não há `flush` nem log por evento. Os testes unitários simulam relógio e producer; as taxas efetivas do MacBook dependem de execução real documentada.

MinIO e Parquet não integram o caminho crítico do MVP. Foram deliberadamente adiados para evitar que armazenamento de eventos brutos desvie o foco dos experimentos de Kafka, consumer lag e paralelismo.

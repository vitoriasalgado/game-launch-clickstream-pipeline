# Arquitetura

O gerador publica eventos sintéticos no tópico `game-clickstream-events`, usando `session_id` como chave de partição. Isso preserva a ordem dos eventos dentro da mesma sessão.

### Decisão de particionamento

`session_id` é uma decisão intencional do MVP, e não uma simplificação acidental. O pipeline mede comportamento de navegação e atividade recente, ambos conceitos naturalmente organizados por sessão. A consequência aceita é que sessões diferentes do mesmo `user_id` podem cair em partitions distintas; portanto, não há garantia de ordem entre sessões de um mesmo usuário. Se uma extensão futura exigir uma linha do tempo global por usuário, a chave poderá ser alterada para `user_id`, com o trade-off de concentrar mais tráfego de usuários muito ativos na mesma partition.

Dois consumer groups leem o mesmo tópico de forma independente:

- `campaign-metrics`: produz agregações por página, campanha, tipo de evento e janela de tempo.
- `active-users`: acompanha sessões ou usuários ativos em janelas recentes.

Cada group pode processar, no máximo, uma partition por consumer. Com seis partitions, acrescentar um sétimo consumer ao mesmo group não aumenta seu paralelismo.

PostgreSQL guarda agregados e resultados operacionais dos experimentos. Kafka UI, Prometheus, Grafana e cAdvisor serão usados para observar o estado do pipeline e dos containers.

MinIO e Parquet não integram o caminho crítico do MVP. Foram deliberadamente adiados para evitar que armazenamento de eventos brutos desvie o foco dos experimentos de Kafka, consumer lag e paralelismo.

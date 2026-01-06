## Planing

Fluxo de processamento de notícias:
1. Le os feeds e procura as urls dos artigos - News harvester
2. Busca o texto de cada artigo - News harvester
3. Vetoriza os artigos, sava no banco e salva na fila de processamento - News harvester
4. Processa possíveis candidatos para clustering
5. Editorial manual
6. Augumenta os artigos com LLM
7. Clusteriza os artigos
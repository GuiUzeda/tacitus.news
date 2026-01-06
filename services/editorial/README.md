## Planing

This is the editorial process

1. news_getter: Gets raw text, performs the vector and entity calculation
2. news_filter: Using gemma 4b filters the relevant headlines
3. news_cluster: Clursters the news articles into news events
4. cli.py: This is a manual process to analyse and weed out ambiguous clusters
5. news_enchancer: enchances the information on the articles, performs bias analysis and summarises the envets
6. cli.py: This is a manual process to get the recently create events and publish, managing the front end main content

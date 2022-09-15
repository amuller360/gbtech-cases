# Case GB Tech 
Repositório contendo ETLs criados de acordo com especificação compartilhada pela gb.tech_

## DAG Vendas
`vendas.py`
`jobs\file_to_db.py`
DAG com objetivo de ler os arquivos recebidos e inserir em um DB da escolha.
Atualmente utilizando PostgreSQL, no entanto será migrado para BigQuery.

## DAG Consolidado
`consolidado.py`
`jobs\calculos.py`
DAG com objetivo de calcular o consolidado de venda de acordo com agregações especificadas pela gb.tech_

## DAG Twitter Trends
`twitter_trends.py`
`jobs\get_twitter.py`
DAG com objetivo de coletar informações no twitter relacionadas ao levantamento realizado  na DAG consolidado.
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, Integer, Date, MetaData

import pandas as pd
import sqlalchemy

#https://datazenit.com/heroku-data-explorer.html
#Connect with Postgres
def connect_toPostgres(schema):
  username = '<HIDE>'
  password = '<HIDE>'
  host = '<HIDE>'
  port = '<HIDE>'
  dbname = '<HIDE>'
  engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')

  print('Connected to Postgres.')

  return engine

#Create Table (Generic)
def create_table(engine, table_name, schema, table):
  try:
    table.drop(engine)
  except:
    print(f'{table} already deleted.')

  table.create(engine)

  print(f'Table {table} created.')

#Create all consolidado tables needed for the case
def create_tables_consolidado(engine,schema):
  tables = []
  metadata = MetaData(schema=schema)

  table_name = 'consolidado_ano_mes'
  table = Table(table_name, metadata,
                    Column('ano_mes', Date),
                    Column('total_vendas', Integer))
  create_table(engine,table_name,schema,table)
  tables.append(table_name)

  table_name = 'consolidado_marca_linha'
  table = Table(table_name, metadata,
                    Column('marca', String(255)),
                    Column('linha', String(255)),
                    Column('total_vendas', Integer))
  create_table(engine,table_name,schema,table)
  tables.append(table_name)

  table_name = 'consolidado_marca_ano_mes'
  table = Table(table_name, metadata,
                    Column('marca', String(255)),
                    Column('ano_mes', Date),
                    Column('total_vendas', Integer))
  create_table(engine,table_name,schema,table)
  tables.append(table_name)

  table_name = 'consolidado_linha_ano_mes'
  table = Table(table_name, metadata,
                    Column('linha', String(255)),
                    Column('ano_mes', Date),
                    Column('total_vendas', Integer))
  create_table(engine,table_name,schema,table)
  tables.append(table_name)

  return tables

#Insert into the DB based on aggregation defined
def insert_into_vendas(engine, schema, table_name, columns, group_by, order_by):
  query = f'INSERT INTO {schema}.{table_name} \
            SELECT {columns} \
            FROM gbtech.vendas \
            GROUP BY {group_by} \
            ORDER BY {order_by}'
  
  print(f'To execute SQL statement: \n {query}')

  result_set = engine.execute(query)
                             
  print(f'Data inserted at {schema}.{table_name}.')

def calculating(engine, schema):
  #Consolidado Ano Mês
  table_name = 'consolidado_ano_mes'
  columns = 'date_trunc(\'month\', data_venda) AS ano_mes, \
             SUM(qtd_venda) AS total_vendas'
  group_by = 'date_trunc(\'month\', data_venda)'
  order_by = '1'
  insert_into_vendas(engine, schema, table_name, columns, group_by, order_by)

  #Consolidado Marca Linha
  table_name = 'consolidado_marca_linha'
  columns = 'marca, linha, \
             SUM(qtd_venda) AS total_vendas'
  group_by = 'marca, linha'
  order_by = '1, 2'
  insert_into_vendas(engine, schema, table_name, columns, group_by, order_by)

  #Consolidado Marca Ano Mes
  table_name = 'consolidado_marca_ano_mes'
  columns = 'marca, \
             date_trunc(\'month\', data_venda) AS ano_mes, \
             SUM(qtd_venda) AS total_vendas'
  group_by = 'marca,  date_trunc(\'month\', data_venda)'
  order_by = '1, 2'
  insert_into_vendas(engine, schema, table_name, columns, group_by, order_by)

  #Consolidado Linha Ano Mes
  table_name = 'consolidado_linha_ano_mes'
  columns = 'linha, \
             date_trunc(\'month\', data_venda) AS ano_mes, \
             SUM(qtd_venda) AS total_vendas'
  group_by = 'linha,  date_trunc(\'month\', data_venda)'
  order_by = '1, 2'
  insert_into_vendas(engine, schema, table_name, columns, group_by, order_by)

def run():
  engine = connect_toPostgres('gbtech')
  create_tables_consolidado(engine,'gbtech')
  calculating(engine,'gbtech')
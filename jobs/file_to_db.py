#!pip install -U -q PyDrive
#!pip install xlrd
#!pip install gcsfs

from google.cloud import storage

from sqlalchemy import create_engine
from sqlalchemy.sql import text as sa_text
from sqlalchemy import Table, Column, String, Integer, Date, MetaData

import pandas as pd
import sqlalchemy

#https://datazenit.com/heroku-data-explorer.html
#Connect with Postgres
def connect_toPostgres(schema):
  username = 'mthnlktoxoaobc'
  password = '7d9feab94a8dc2622c655abd81b11c678806ea764e898ed7f11c59fb88e3c93b'
  host = 'ec2-35-168-122-84.compute-1.amazonaws.com'
  port = '5432'
  dbname = 'd1fsodgvbpvkk6'
  engine = create_engine(f'postgresql://{username}:{password}@{host}:{port}/{dbname}')

  schema = schema
  if not engine.dialect.has_schema(engine, schema):
      engine.execute(sqlalchemy.schema.CreateSchema(schema))

  print('Connected to Postgres.')

  return engine

#Function Create Table Vendas
def create_table_vendas(engine, table_name, schema):
  table_name = table_name

  metadata = MetaData(schema=schema)
  table = Table(table_name, metadata,
                    Column('id_marca', Integer),
                    Column('marca', String(255)),
                    Column('id_linha', Integer),
                    Column('linha', String(255)),
                    Column('data_venda', Date),
                    Column('qtd_venda', Integer))

  try:
    table.drop(engine)
  except:
    print(f'{table} already deleted.')

  table.create(engine)

  print(f'Table {table} created.')

  return table

#Open Files and Append to STG
def append_files_toSTG(client, engine, schema):
  table_name = 'vendas_stg'
  vendas_stg = create_table_vendas(engine,table_name,schema)

  for blob in client.list_blobs('gbtech-cases-amuller', prefix='vendas/Base'):   
    df = pd.read_csv(f'gs://gbtech-cases-amuller/{blob.name}')
    df.columns = df.columns.str.lower()
    df.to_sql(table_name, engine,
              schema = schema,
              if_exists = 'append',
              index = False)
    
    print(f'File {blob.name} appended to table {table_name}.')
    
  return vendas_stg

#Create Final Table with Distict Records
def create_table(engine, schema, stg):
  vendas_stg = stg
  vendas = create_table_vendas(engine,'vendas',schema)

  engine.execute(f'INSERT INTO {vendas} \
                  SELECT DISTINCT id_marca, marca, id_linha, \
                                  linha, data_venda, qtd_venda \
                  FROM {vendas_stg}')
  
  print(f'Data inserted at {vendas}.')

def run():
  engine = connect_toPostgres('gbtech')
  client = storage.Client()
  stg = append_files_toSTG(client, engine, 'gbtech')
  create_table(engine, 'gbtech', stg)
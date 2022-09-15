#!pip install --upgrade tweepy

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, String, Integer, Date, MetaData

import tweepy
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

def get_keywords(engine):
  query = f'WITH linhas_2019 AS ( \
               SELECT linha, total_vendas, \
                      ROW_NUMBER() OVER (ORDER BY total_vendas DESC) AS n \
               FROM gbtech.consolidado_linha_ano_mes WHERE ano_mes = \'2019-12-01\' \
          ) SELECT linha FROM linhas_2019 WHERE n = 1'

  result_set = engine.execute(query).first()
  keywords = f'boticario {result_set[0].lower()}'

  return keywords

#https://dev.to/twitterdev/a-comprehensive-guide-for-using-the-twitter-api-v2-using-tweepy-in-python-15d9
#https://github.com/twitterdev/getting-started-with-the-twitter-api-v2-for-academic-research/blob/main/modules/5-how-to-write-search-queries.md
#Get Tweets and Return DF with them
def get_tweets(keywords):
  bearer_token = '<HIDE>'
  client = tweepy.Client(bearer_token=bearer_token)

  query = f'{keywords} lang:pt'

  result = client.search_recent_tweets(query=query, 
                                      tweet_fields=['author_id','lang', 'created_at'], 
                                      user_fields=['username'],
                                      expansions=['entities.mentions.username','author_id'],
                                      max_results=50)
  
  print(f'Querying tweets using keywords: {keywords}.')

  tweets = []
  for row in result.data:
    tweet = row.text
    created_at = row.created_at

    for user in result.includes['users']:
      if user.id == row.author_id:
        username = user.username

    tweets.append([username,tweet,created_at])

  tweets_df = pd.DataFrame(tweets, columns=['username','tweet','created_at'])

  print(f'Tweet DF created.')
  return tweets_df

#Create Table with Tweets Recovered
def create_tweets_table(engine, schema, tweets_df):
  table_name = 'tweets'

  metadata = MetaData(schema=schema)
  table = Table(table_name, metadata,
                    Column('username', String),
                    Column('tweet', String),
                    Column('created_at', Date))
  try:
    table.drop(engine)
  except:
    print(f'{table} already deleted.')

  table.create(engine)

  print(f'Table {schema}.{table_name} created.')

  tweets_df.to_sql('tweets', engine,
                 schema = schema,
                 if_exists = 'append',
                 index = False)
  
  print(f'Data inserted at {schema}.{table_name}.')

def run():
  engine = connect_toPostgres('gbtech')
  keywords = get_keywords(engine)
  tweets_df = get_tweets(keywords)
  create_tweets_table(engine,'gbtech',tweets_df)
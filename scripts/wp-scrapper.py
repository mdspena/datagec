import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import re
import time
import requests
import pandas as pd
from bs4 import BeautifulSoup

args = getResolvedOptions(sys.argv, ['JOB_NAME','baseurl','bucketname','database'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

print(args)
base_url = args['baseurl']
bucket_name = args['bucketname']
database = args['database']

def scrap(base_url):
    page = 1
    post_list = []
    
    while True:
        print(f'Carregando página {page}...')
    
        params = {
            'per_page': 100,
            'page': page,
            '_embed': 'author,wp:term'
        }
    
        response = requests.get(base_url, params=params)
    
        if response.status_code == 200:
            posts = response.json()
            if not posts:
                break
    
            for post in posts:
                link = post['link']
                title = post['title']['rendered']
                content = post['content']['rendered']
                user = post['_embedded']['author'][0]['name']
                date = post['date']
    
                tags = []
                terms = post['_embedded'].get('wp:term', [])
                for term_group in terms:
                    for term in term_group:
                        if term.get('taxonomy') == 'post_tag':
                            tags.append(term['name'])
    
                post_dict = dict()
                post_dict['link'] = link
                post_dict['title'] = title
                post_dict['content'] = content
                post_dict['user'] = user
                post_dict['date'] = date
                post_dict['tags'] = tags
                post_list.append(post_dict)
    
            page += 1
            time.sleep(1)
    
        else:
            print(response.status_code)
            break
    return post_list

def extract_author(text):
    pattern = r'colaborador(?:es|a|as)?\s+(.*?)\s*</em>'
    match = re.search(pattern, text, flags=re.IGNORECASE)
    if match:
        raw = match.group(1).strip()
        cleaned = re.sub(r'<[^>]+>', '', raw)
        return cleaned.strip()
    return None

def extract_reading_time(text):
    cleaned = re.sub(r'<[^>]+>', ' ', text)
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    if re.search(r'Tempo de leitura:\s*&lt;\s*1\s*minuto', cleaned, flags=re.IGNORECASE):
        return 1
    pattern = r'Tempo de leitura:\s*(\d+)\s*minuto(?:s)?'
    match = re.search(pattern, cleaned, flags=re.IGNORECASE)
    if match:
        return int(match.group(1))
    return None
    
def get_edition(title):
  match = re.search(r'\(V\.\d+, N\.\d+, P\.\d+, \d{4}\)$', title)
  if match:
      print(match.group(0))
  return match.group(0)

post_list = scrap(base_url)

for i,post in enumerate(post_list):
  if post['user'] != 'admin':
    author = [post['user']]
    post_list[i]['user_flag'] = True
  elif extract_author(post['content']) == 'Tales Alexandre Costa e Silva':
    author = ['Tales Alexandre Costa e Silva']
    post_list[i]['user_flag'] = False
  else:
    print(extract_author(post['content']))
    author = re.split(r',\s*|\se\s*', extract_author(post['content']))
    post_list[i]['user_flag'] = False

  post_list[i]['authors'] = author
  post_list[i]['edition'] = get_edition(post['title'])
  post_list[i]['reading_time'] = extract_reading_time(post['content'])

df = spark.createDataFrame(post_list)

path = f's3://{bucket_name}/data/{database}/articles_extract'
df.write \
  .mode('overwrite') \
  .option('compression', 'snappy') \
  .parquet(path)
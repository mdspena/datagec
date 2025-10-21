from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

import sys
import json
import boto3
import awswrangler as wr
import datetime as dt
import pandas as pd
import numpy as np
import pytz
import traceback

from datetime import datetime, timedelta
from google.oauth2 import service_account
from google.cloud import bigquery

args = getResolvedOptions(sys.argv, ['JOB_NAME','secretname','bucketname','database','startdate','enddate','project','dataset'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)
job.commit()

print(args)
secret_name = args['secretname']
bucket_name = args['bucketname']
database = args['database']
start_date = args['startdate']
end_date = args['enddate']
project = args['project']
dataset = args['dataset']

def get_secret_value(secret_name):

    secret_client = boto3.client('secretsmanager')
    secret_string = secret_client.get_secret_value(SecretId=secret_name)['SecretString']
    secret_dict = json.loads(secret_string)

    return secret_dict

def get_dates(start_date, end_date):
    start = datetime.strptime(start_date, '%Y%m%d')
    end = datetime.strptime(end_date, '%Y%m%d')
    current = start
    date_list = []
    while current <= end:
        date_list.append(current.strftime('%Y%m%d'))
        current += timedelta(days=1)
    return date_list

def convert_to_json(x):
    if isinstance(x, (dict, list, np.ndarray)):
        if isinstance(x, np.ndarray):
            x = x.tolist()
        return json.dumps(x)
    return x

def write_table(df, dtype_dict, bucket, database, table_name):
    wr.s3.to_parquet(
        df=df,
        dataset=True,
        mode='overwrite_partitions',
        database=database,
        table=table_name,
        dtype=dtype_dict,
        partition_cols=['extract_date'],
        path=f's3://{bucket}/{database}/{table_name}',
        compression='snappy'
    )
    
credentials = service_account.Credentials.from_service_account_info(get_secret_value(secret_name))
client = bigquery.Client(credentials=credentials)

date_list = get_dates(start_date, end_date)

for extract_date in date_list:

    event_query = f'''SELECT * FROM `{project}.{dataset}.events_{extract_date}`'''
    print('event_query:', event_query)

    events_df = client.query(event_query).to_dataframe()
    events_df['extract_date'] = extract_date
    events_df = events_df.applymap(convert_to_json)

    dtype_dict = {column: 'string' for column in events_df.columns}
    table_name = 'ga4_events'

    write_table(
        events_df,
        dtype_dict,
        bucket_name,
        database,
        table_name
    )
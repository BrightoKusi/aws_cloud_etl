print('hello world')
import configparser
config = configparser.ConfigParser()
config.read('.env')
import boto3 
import pandas as pd
from sqlalchemy import create_engine
import redshift_connector

access_key = config['AWS']['access_key']
secret_key = config['AWS']['secret_key']
bucket_name = config['AWS']['bucket_name']
region = config['AWS']['region']

db_host = config['DB_CONN']['host']
db_user = config['DB_CONN']['user']
db_password = config['DB_CONN']['password']
db_database = config['DB_CONN']['database']





# Step 1. Create the s3 bucket (data lake)

client = boto3.client(
    's3',
    aws_access_key_id= access_key,
    aws_secret_access_key=secret_key
)

response = client.create_bucket(
    Bucket=bucket_name,
    CreateBucketConfiguration={
        'LocationConstraint': region,
    },
)


# Step 2. Extract data from postgres to data lake
conn = engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_password}@{db_host}:5432/{db_database}") 


db_tables = ['banks', 'cards', 'cust_verification_status', 'transaction_status', 'transactions', 'users', 'wallets']

for table in db_tables:
    query = f'SELECT * FROM {table}'
    df = pd.read_sql_query(query, conn)

    df.to_csv(
        f's3://{bucket_name}/{table}.csv'
        ,index= False
        ,storage_options = {
            'key': access_key,
            'secret': secret_key 
        }
        )
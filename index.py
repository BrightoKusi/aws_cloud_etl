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

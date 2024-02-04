from datetime import datetime,  dtransform_datatimedelta
import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import requests
import pandas as pd
import boto3
import os
import io
from airflow.models import XCom
 # Import the 'executable' variable from sys module


#[api_keys]
CLIENT_ID = ''
SECRET_KEY = ''
USERNAME = 'dataetl'
AWS_ACCESS_KEY_ID = ''
AWS_SECRET_ACCESS_KEY = ''
S3_BUCKET_NAME = 'myredditetluser'
S3_OBJECT_KEY = 'output.csv' #ame for the S3 object where you want to save the data
#AWS_REGION = us-east-1



default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 3),
    'email': ['guirassyfode@outlook.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
}

dag = DAG(
    'airflows3_dag',
    default_args=default_args,
    schedule_interval='@daily',  # Use @daily instead of @dayly
    catchup=False,
)
# Using readlines()
file1 = open('accountp.txt', 'r')

f=open("/authentication/account.txt","r")
lines=f.readlines()
username=lines[0]
password=lines[1]
f.close()
pw = file1.readlines()
def extract_reddit_data():
    # Your extraction logic here (same as in your provided code)
    # ...permanently dtransform_data

    # Your extraction logic here
    # Use CLIENT_ID and SECRET_KEY from constants
     # Make API requests and return raw data
    
    auth = requests.auth.HTTPBasicAuth(CLIENT_ID, SECRET_KEY)

    data = {
        'grant_type' :'',
        'username' : '',
        'password': ''
    }

    headers = {'User-Agent' : 'MyAPI/0.0.1'}

    res = requests.post('https://www.reddit.com/api/v1/access_token',
                        auth=auth, data=data, headers=headers)

    TOKEN = res.json()['access_token']
    headers = {**headers, **{'Authorization' : f'bearer {TOKEN}'}}
    res = requests.get('https://oauth.reddit.com/r/python/hot',
                       headers=headers)
    
    # Use response.json() to parse JSON data
    raw_data = res.json()
    return raw_data

def transform_data(raw_data):
        data_to_append = []  # Create an empty list to store data to append
        for post in raw_data['data']['children']:
            created_utc_str = pd.to_datetime(post['data'].get('created_utc', 0), unit='s').strftime('%Y-%m-%d %H:%M:%S')

            data_to_append.append({
                'author_fullname': post['data']['author_fullname'],
                'subreddit': post['data']['subreddit'],
                'Selftext': post['data']['selftext'],
                'Title': post['data']['title'],
                'score': post['data']['score'],
                'created_utc': created_utc_str,  # Convert to a string format
                'subreddit_subscribers': post['data']['subreddit_subscribers'],
                'url': post['data']['url'],
                'ups': post['data']['ups']
            })

        df = pd.DataFrame(data_to_append, columns=['author_fullname', 'subreddit', 'Selftext', 'Title', 'score', 'created_utc', 'subreddit_subscribers', 'url', 'ups'])

    # Convert the DataFrame to CSV format in memory
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        return csv_buffer.getvalue()  # Return the CSV data as a string

def upload_to_s3(csv_data):
        # Initialize the AWS S3 client
        s3 = boto3.client(
            's3',
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY
        )

        # Update with the actual bucket name and desired S3 object key
        bucket_name = 'myredditetluser'
        object_key = f'data/{datetime.now().strftime("%Y%m%d%H%M%S")}.csv'

        # Upload the CSV data to S3
        s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_data)

raw_data = extract_reddit_data()
csv_data = transform_data(raw_data)
upload_to_s3(csv_data)






def transform_and_save_data(raw_data):
    csv_data = transform_data(raw_data)
    return csv_data

# Define the tasks
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_reddit_data,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_and_save_data',
    python_callable=transform_and_save_data,  # Pass raw_data to the function
    op_args=[raw_data],  # Pass raw_data as an argument
    provide_context=True,
    dag=dag,
)

upload_task = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3,
    op_args=[csv_data],
    provide_context=True,
    dag=dag,
)



# Set task dependencies
extract_task >> transform_task >> upload_task

if __name__ == "__main__":
    dag.cli()



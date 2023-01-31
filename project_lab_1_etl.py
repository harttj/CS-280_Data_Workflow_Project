from airflow import DAG
from airflow.models import TaskInstance, Variable
import logging
import pendulum
from airflow.operators.python import PythonOperator

import requests
import pandas as pd
from google.cloud import storage
import gcsfs
from databox import Client


def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f'Bearer {my_bearer_token}'}
user_ids = Variable.get("TWITTER_USER_IDS", [], deserialize_json=True)
tweet_ids = Variable.get("TWITTER_TWEET_IDS", [], deserialize_json=True)
databox_token = Variable.get("DATABOX_TOKEN")


def get_twitter_api_data_func(ti: TaskInstance, user_ids=user_ids,tweet_ids=tweet_ids, **kwargs):
    # Get the data from the list of user ids
    user_requests = []
    for id in user_ids:
        api_url = f'https://api.twitter.com/2/users/{id}'
        request = requests.get(api_url, headers=get_auth_header(),params={'user.fields':'public_metrics,profile_image_url,username,description,id'})
        user_requests.append(request.json()['data'])

    # Get the data from the list of tweet ids
    tweet_requests = []
    for id in tweet_ids:
        api_url = f'https://api.twitter.com/2/tweets/{id}'
        request = requests.get(api_url, headers=get_auth_header(),params={'tweet.fields':'public_metrics,author_id,text'})
        tweet_requests.append(request.json()['data'])

    # Pass the user data
    ti.xcom_push("user_info",user_requests)
    # Pass the tweet data
    ti.xcom_push("tweet_info",tweet_requests)
    logging.info('user_info: ', user_requests, 'tweet_info: ', tweet_requests)

def transform_twitter_api_data_func(ti: TaskInstance, **kwargs):
    user_data = ti.xcom_pull(key='user_info',task_ids='get_twitter_api_data_task')
    tweet_data = ti.xcom_pull(key='tweet_info',task_ids='get_twitter_api_data_task')

    user_df=pd.DataFrame.from_dict(user_data)
    user_df =pd.concat([user_df.drop(['profile_image_url','description','public_metrics'],axis=1),user_df['public_metrics'].apply(pd.Series)],axis=1)
    user_df.rename(columns={'id': 'user_id'},inplace=True)

    tweet_df=pd.DataFrame.from_dict(tweet_data)
    tweet_df = pd.concat([tweet_df.drop(['edit_history_tweet_ids','author_id','public_metrics'],axis=1),tweet_df['public_metrics'].apply(pd.Series)],axis=1)
    tweet_df.rename(columns={'id': 'tweet_id'},inplace=True)

    client = storage.Client()
    bucket = client.get_bucket("t-h-apache-airflow-cs280")
    bucket.blob("data/user.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")
    
def load_twitter_api_data_func(ti:TaskInstance):
    client = storage.Client()
    bucket = client.get_bucket("t-h-apache-airflow-cs280")
    fs = gcsfs.GCSFileSystem(project='TJ-Hart-CS-280')
    with fs.open('t-h-apache-airflow-cs280/data/user.csv') as user:
        user_df = pd.read_csv(user)
    with fs.open('t-h-apache-airflow-cs280/data/tweet.csv') as tweet:
        tweet_df = pd.read_csv(tweet)
    client = Client(f'{databox_token}')
    for index in user_df.index.values:
        name = user_df.loc[index,'name'].replace(' ', '_')
        followers_count = int(user_df.loc[index,'followers_count'])
        following_count = int(user_df.loc[index,'following_count'])
        tweet_count = int(user_df.loc[index,'tweet_count'])
        listed_count = int(user_df.loc[index,'listed_count'])
        logging.info(name,'\n',followers_count,'\n',following_count,'\n',tweet_count,'\n',listed_count)
        client.push(f"{name}s_followers_count", followers_count)
        client.push(f"{name}s_following_count", following_count) 
        client.push(f"{name}s_tweet_count", tweet_count) 
        client.push(f"{name}s_listed_count", listed_count) 
    for index in tweet_df.index.values:
        tweet_id = int(tweet_df.loc[index,'tweet_id'])
        reply_count = int(tweet_df.loc[index,'reply_count'])
        like_count = int(tweet_df.loc[index,'like_count'])
        quote_count = int(tweet_df.loc[index,'quote_count'])
        retweet_count = int(tweet_df.loc[index,'retweet_count'])
        client.push(f"{tweet_id}_reply_count", reply_count)
        client.push(f"{tweet_id}_like_count", like_count) 
        client.push(f"{tweet_id}_quote_count", quote_count) 
        client.push(f"{tweet_id}_retweet_count", retweet_count)    
with DAG(
        dag_id="project_lab_1_etl",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 1, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    get_twitter_api_data_task = PythonOperator(task_id="get_twitter_api_data_task", python_callable=get_twitter_api_data_func,provide_context=True)
    transform_twitter_api_data_task = PythonOperator(task_id="transform_twitter_api_data_task", python_callable=transform_twitter_api_data_func, provide_context=True)
    load_twitter_api_data_task = PythonOperator(task_id="load_twitter_api_data_task", python_callable=load_twitter_api_data_func, provide_context=True)
get_twitter_api_data_task >> transform_twitter_api_data_task >> load_twitter_api_data_task

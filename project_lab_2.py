from airflow import DAG
from airflow.models import TaskInstance, Variable
import logging
import pendulum
from airflow.operators.python import PythonOperator

import requests
import pandas as pd
from google.cloud import storage
from datetime import datetime
import gcsfs

from models.config import Session #You would import this from your config file
from models.User import User
from models.Tweet import Tweet
from models.User_Timeseries import User_Timeseries
from models.Tweet_Timeseries import Tweet_Timeseries


def get_auth_header():
    my_bearer_token = Variable.get("TWITTER_BEARER_TOKEN")
    return {"Authorization": f'Bearer {my_bearer_token}'}


def load_data_func(ti: TaskInstance, **kwargs):
    print("Hello World!")
    session = Session()

    # This will retrieve all of the users from the database 
    # (It'll be a list, so you may have 100 users or 0 users)
    user_tuple_list = session.query(User.user_id).all()
    user_id_list = [user_id[0] for user_id in user_tuple_list]
    tweet_tuple_list = session.query(Tweet.tweet_id).all()  
    print(tweet_tuple_list)
    tweet_id_list = [int(tweet_id[0]) for tweet_id in tweet_tuple_list]
    print(user_id_list,tweet_id_list)

    # Pass the user data
    ti.xcom_push("user_id",user_id_list)
    # Pass the tweet data
    ti.xcom_push("tweet_id",tweet_id_list)
    logging.info('user_id: ', user_id_list, 'tweet_id: ', tweet_id_list)

    # This will close the session that you opened at the beginning of the file.
    session.close()

def call_api_func(ti: TaskInstance, **kwargs):
    session = Session()
    user_ids = ti.xcom_pull(key='user_id',task_ids='load_data_task')
    tweet_ids = ti.xcom_pull(key='tweet_id',task_ids='load_data_task')
    user_requests = []
    tweet_requests = []

    for user_id in user_ids:
        api_url = f'https://api.twitter.com/2/users/{user_id}'
        request = requests.get(api_url, headers=get_auth_header(),params={'user.fields':'id,public_metrics,username,name,created_at'})
        user_requests.append(request.json()['data'])
        tweets_url = f'https://api.twitter.com/1.1/statuses/user_timeline.json'
        last_5_tweets = requests.get(tweets_url, headers=get_auth_header(), params={"user_id": user_id, "count": 5})
        users_recent_tweets = last_5_tweets.json()
        print(users_recent_tweets)
        for dict in users_recent_tweets:
            keys=['id','text','text','created_at','retweet_count','favorite_count']
            tweet = {key: dict[key] for key in keys}
            tweet['user_id'] = user_id
            tweet_requests.append(tweet)


    # Get the data from the list of tweet ids
    for tweet_id in tweet_ids:
        api_url = f'https://api.twitter.com/2/tweets/{tweet_id}'
        request = requests.get(api_url, headers=get_auth_header(),params={'tweet.fields':'public_metrics,author_id,text'})
        tweet=request.json()['data']
        # tweet['public_metrics']['favorite_count'] = tweet['public_metrics'].pop('like_count')
        tweet['public_metrics']['retweet_count_pm'] = tweet['public_metrics'].pop('retweet_count')
        tweet_requests.append(tweet)

    # Pass the user data
    ti.xcom_push("user_info",user_requests)
    # Pass the tweet data
    ti.xcom_push("tweet_info",tweet_requests)
    logging.info('user_info: ', user_requests, 'tweet_info: ', tweet_requests)   
    # This will close the session that you opened at the beginning of the file.
    session.close()

def transform_data_func(ti: TaskInstance, **kwargs):
    session = Session()
    user_data = ti.xcom_pull(key='user_info',task_ids='call_api_task')
    tweet_data = ti.xcom_pull(key='tweet_info',task_ids='call_api_task')

    user_df=pd.DataFrame(user_data)
    user_df['date'] = datetime.now().strftime("%Y-%m-%d")
    user_df =pd.concat([user_df.drop(['public_metrics'],axis=1),user_df['public_metrics'].apply(pd.Series)],axis=1)
    user_df.rename(columns={'id': 'user_id'},inplace=True)

    tweet_df=pd.DataFrame(tweet_data)
    tweet_df['date'] = datetime.now().strftime("%Y-%m-%d")
    tweet_df.rename(columns={'id': 'tweet_id'},inplace=True)
    tweet_df =pd.concat([tweet_df.drop(['public_metrics'],axis=1),tweet_df['public_metrics'].apply(pd.Series)],axis=1)
    tweet_df['favorite_count'] = tweet_df['favorite_count'].fillna(tweet_df['like_count'])
    tweet_df['retweet_count'] = tweet_df['retweet_count'].fillna(tweet_df['retweet_count_pm'])
    tweet_df['user_id'] = tweet_df['user_id'].fillna(tweet_df['author_id'])

    client = storage.Client()
    bucket = client.get_bucket("t-h-apache-airflow-cs280")
    bucket.blob("data/user_df.csv").upload_from_string(user_df.to_csv(index=False), "text/csv")
    bucket.blob("data/tweet_df.csv").upload_from_string(tweet_df.to_csv(index=False), "text/csv")
    # This will close the session that you opened at the beginning of the file.
    session.close()

def write_data_func(ti:TaskInstance):
    session = Session()
    # This will close the session that you opened at the beginning of the file.
    client = storage.Client()
    fs = gcsfs.GCSFileSystem(project='TJ-Hart-CS-280')
    with fs.open('t-h-apache-airflow-cs280/data/user_df.csv') as user:
        user_df = pd.read_csv(user)
    with fs.open('t-h-apache-airflow-cs280/data/tweet_df.csv') as tweet:
        tweet_df = pd.read_csv(tweet)
    all_user_tseries = []
    all_tweet_tseries = []
    tweets_to_add  = []
    for index in user_df.index.values:
        df_user_id = int(user_df.loc[index,'user_id'])
        df_followers_count = int(user_df.loc[index,'followers_count'])
        df_following_count = int(user_df.loc[index,'following_count'])
        df_tweet_count = int(user_df.loc[index,'tweet_count'])
        df_listed_count = int(user_df.loc[index,'listed_count'])
        df_date = user_df.loc[index,'date']
        user_timeseries = User_Timeseries(
                                            user_id = df_user_id,
                                            followers_count = df_followers_count,
                                            following_count = df_following_count,
                                            tweet_count = df_tweet_count,
                                            listed_count = df_listed_count,
                                            date = df_date,
                                        )
        all_user_tseries.append(user_timeseries)
    session.add_all(all_user_tseries)
    for index in tweet_df.index.values:
        df_tweet_id = int(tweet_df.loc[index,'tweet_id'])
        df_user_id = int(tweet_df.loc[index,'user_id'])
        df_favorite_count = int(tweet_df.loc[index,'favorite_count'])
        df_retweet_count = int(tweet_df.loc[index,'retweet_count'])
        df_date = tweet_df.loc[index,'date']
        df_text = tweet_df.loc[index,'text']
        df_created_at = tweet_df.loc[index,'created_at']
        tweet_timeseries = Tweet_Timeseries(
                                            tweet_id = df_tweet_id,
                                            favorite_count = df_favorite_count,
                                            retweet_count = df_retweet_count,
                                            date = df_date,
                                        )
        all_tweet_tseries.append(tweet_timeseries)
        print("tweet.filter",session.query(Tweet).filter(Tweet.tweet_id == df_tweet_id).first(),bool(session.query(Tweet).filter(Tweet.tweet_id == df_tweet_id).first()))
        if not bool(session.query(Tweet).filter(Tweet.tweet_id == df_tweet_id).first()):
            tweet = Tweet(
                        tweet_id = df_tweet_id,
                        user_id = df_user_id,
                        text = df_text,
                        created_at = df_created_at
                        )
            tweets_to_add.append(tweet)
    session.add_all(all_tweet_tseries)
    session.add_all(tweets_to_add)
    session.commit()

    session.close()
    



with DAG(
        dag_id="project_lab_2",
    schedule_interval="0 9 * * *",
    start_date=pendulum.datetime(2023, 2, 26, tz="US/Pacific"),
    catchup=False,
) as dag:
    load_data_task = PythonOperator(task_id="load_data_task", python_callable=load_data_func,provide_context=True)
    call_api_task = PythonOperator(task_id="call_api_task", python_callable=call_api_func, provide_context=True)
    transform_data_task = PythonOperator(task_id="transform_data_task", python_callable=transform_data_func, provide_context=True)
    write_data_task = PythonOperator(task_id="write_data_task", python_callable=write_data_func, provide_context=True)
load_data_task >> call_api_task >> transform_data_task >> write_data_task
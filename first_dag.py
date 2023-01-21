from airflow import DAG
import logging as log
import pendulum
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

def first_task_function():
    log.info("Welcome to CS 280! This is your first task")
    name = "TJ Hart"
    log.info(f"My name is {name}")
    return

def second_task_function():
    log.info("This is your second task")
    major = "Applied Physics"
    log.info(f"My major is {major}")
    return

def third_task_function():
    log.info("This is your third task")
    hometown = "Mapleton"
    log.info(f"I am from {hometown}")
    return

with DAG(
    dag_id="My First CS 280 DAG",
    schedule_interval="0 10 * * *",
    start_date=pendulum.datetime(2023, 9, 1, tz="US/Pacific"),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")
    first_task = PythonOperator(task_id="first_task", python_callable=first_task_function)
    second_task = PythonOperator(task_id="second_task", python_callable=second_task_function)
    third_task = PythonOperator(task_id="third_task", python_callable=third_task_function)
    end_task = DummyOperator(task_id="end_task")

start_task >> first_task >> second_task >> third_task >> end_task

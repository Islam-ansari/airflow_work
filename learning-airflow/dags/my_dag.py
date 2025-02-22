from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator

def print_a():
    print('Hello World!')

def print_b():
    print('Hello World!')

def print_c():
    print('Hello World!')

def print_d():
    print('Hello World!')

with DAG('my_dag', start_date=datetime(2024, 2, 19),
         description='A simple tutorial DAG',
         tags=['Data Science'],
         schedule='@daily',
         catchup=False): 
    
    task_a = PythonOperator(task_id='task_a', python_callable=print_a)
    task_b = PythonOperator(task_id='task_b', python_callable=print_b)
    task_c = PythonOperator(task_id='task_c', python_callable=print_c)
    task_d = PythonOperator(task_id='task_d', python_callable=print_d)


    task_a >> [task_b, task_c] >> task_d
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
#from airflow.operators.python_operator import chain  # Importa chain desde python_operator
from api import read_df_unido,transform_API
from carros import read_csv, transform_datos
from cargar import load
#from kafka import enviar_datos
#from merge import merge,load
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 9, 13),  # Update the start date to today or an appropriate date
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

def func1():
    print(f"the date is: {datetime.now()}")

with DAG(
    'api__project_dag',
    default_args=default_args,
    description='Our first DAG with ETL process!',
    schedule_interval='@daily',  # Set the schedule interval as per your requirements

) as dag:

   # merge = PythonOperator(
   #     task_id='merge',
    #    python_callable=merge,
     ##   provide_context = True,
      #  )

    read_api = PythonOperator(
        task_id='read_api',
        python_callable=read_df_unido,
        provide_context = True,
        )

    transform_api = PythonOperator(
        task_id='transform_api',
        python_callable=transform_API,
        provide_context = True,
        )

    read_carros = PythonOperator(
        task_id='read_carros',
        python_callable=read_csv,
        provide_context = True,
        )

    transform_carros = PythonOperator(
        task_id='transform_carros',
        python_callable=transform_datos,
        provide_context = True,
        )

    store = PythonOperator(
        task_id='load',
        python_callable=load,
        provide_context = True,
        )
    load_api = PythonOperator(
        task_id ='load_api',
        python_callable = func1,
        provide_context = True,
        )

   # kafka = PythonOperator(
    #    task_id='kafka',
     #   python_callable=kafka,
      #  provide_context = True,
      #  )
    

    read_api >> transform_api  >> load_api
    read_carros >> transform_carros >> store
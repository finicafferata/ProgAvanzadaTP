from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta 

from pipeline_logic import (
    filtrar_datos_task_callable,
    top_ctr_task_callable,
    top_product_task_callable,
    db_writing_task_callable
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='AdTech_Recommendation_Pipeline_V2',
    default_args=default_args,
    description='Pipeline diario para generar recomendaciones de productos AdTech',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['adtech', 'recommendations'],
) as dag:
    
    task_filtrar_datos = PythonOperator(
        task_id='filtrar_datos_del_dia',
        python_callable=filtrar_datos_task_callable,
    )

    task_top_ctr = PythonOperator(
        task_id='calcular_top_ctr',
        python_callable=top_ctr_task_callable,
    )

    task_top_product = PythonOperator(
        task_id='calcular_top_product',
        python_callable=top_product_task_callable,
    )

    task_db_writing = PythonOperator(
        task_id='escribir_recomendaciones_a_db',
        python_callable=db_writing_task_callable,
    )

    task_filtrar_datos >> [task_top_ctr, task_top_product]
    [task_top_ctr, task_top_product] >> task_db_writing
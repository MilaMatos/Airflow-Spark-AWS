from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from executar_spark import executar_spark
import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def iniciar_spark(path, **kwargs):
    executar_spark(path)

# Configurações padrão da DAG
default_args = {
    'owner': 'milamatos',
    'start_date': datetime(2023, 12, 6),
    'retries': 0
}

customers_bronze_path = "pysparks/bronze_transform_customers.py"
orders_items_bronze_path = "pysparks/bronze_transform_orders_items.py"
orders_bronze_path = "pysparks/bronze_transform_orders.py"

customers_silver_path = "pysparks/silver_transform_customers.py"
orders_items_silver_path = "pysparks/silver_transform_orders_items.py"
orders_silver_path = "pysparks/silver_transform_orders.py"

gold_path = "pysparks/gold_transform.py"

with DAG(
    'Tasks_pipeline_aws',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    customers_bronze = PythonOperator(
        task_id='customers_bronze',
        python_callable=iniciar_spark,
        op_args=[customers_bronze_path]
    )

    orders_items_bronze = PythonOperator(
        task_id='orders_items_bronze',
        python_callable=iniciar_spark,
        op_args=[orders_items_bronze_path]
    )

    orders_bronze = PythonOperator(
        task_id='orders_bronze',
        python_callable=iniciar_spark,
        op_args=[orders_bronze_path]
    )

    customers_silver = PythonOperator(
        task_id='customers_silver',
        python_callable=iniciar_spark,
        op_args=[customers_silver_path]
    )

    orders_items_silver = PythonOperator(
        task_id='orders_items_silver',
        python_callable=iniciar_spark,
        op_args=[orders_items_silver_path]
    )

    orders_silver = PythonOperator(
        task_id='orders_silver',
        python_callable=iniciar_spark,
        op_args=[orders_silver_path]
    )

    gold = PythonOperator(
        task_id='gold_task',
        python_callable=iniciar_spark,
        op_args=[gold_path]
    )


    # Definindo as dependências entre as tarefas
    customers_bronze >> customers_silver
    orders_bronze >> orders_silver
    orders_items_bronze >> orders_items_silver

    customers_silver >> gold
    orders_silver >> gold
    orders_items_silver >> gold

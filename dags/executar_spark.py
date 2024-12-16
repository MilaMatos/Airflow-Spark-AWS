import subprocess
from airflow.hooks.base import BaseHook
import os

base_path = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

def executar_spark(path_pyspark):
    try:
        # Obter credenciais AWS da conexão do Airflow
        aws_conn = BaseHook.get_connection('aws_default')
        aws_access_key = aws_conn.login
        aws_secret_key = aws_conn.password

        print(path_pyspark)
        print(os.path.join(base_path, path_pyspark))

        spark_submit_command = f"""
        export AWS_ACCESS_KEY_ID={aws_access_key};
        export AWS_SECRET_ACCESS_KEY={aws_secret_key};
        spark-submit --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.508 \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=com.amazonaws.auth.DefaultAWSCredentialsProviderChain \
        --conf spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com \
        --conf spark.hadoop.fs.s3a.region=us-east-1 \
        "{os.path.join(base_path,path_pyspark)}"
        """

        # Executar o comando
        subprocess.run(spark_submit_command, shell=True, check=True, executable="/bin/bash")

        print("Spark-submit executado com sucesso!")

    except subprocess.CalledProcessError as e:
        print(f"Erro ao executar spark-submit: {e}")
        raise e

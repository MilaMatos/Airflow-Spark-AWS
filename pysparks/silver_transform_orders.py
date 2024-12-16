from pyspark.sql import SparkSession

# Inicializar uma sessão Spark com configuração para S3
spark = SparkSession.builder \
    .appName("SilverTransform_Orders") \
    .config("spark.hadoop.fs.s3a.access.key", "SEU_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SEU_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.session.token", "SEU_SESSION_TOKEN") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

try:
    # Caminhos S3 para leitura e escrita
    bucket_name = "cm-airflow-spark"
    orders_path_bronze = f"s3a://{bucket_name}/bronze/orders"
    orders_path_silver = f"s3a://{bucket_name}/silver/orders"

    # Ler dados do S3 na camada bronze
    silver_orders = spark.read.parquet(orders_path_bronze)

    # Transformar os dados
    orders_prefix = "order_"
    silver_orders = silver_orders.toDF(*[col.replace(orders_prefix, "") for col in silver_orders.columns])

    # Salvar os dados transformados na camada silver no S3
    silver_orders.write.mode("overwrite").parquet(orders_path_silver)

    print("Transformação concluída e dados salvos na camada silver com sucesso!")

except Exception as e:
    print(f"Erro durante a execução: {e}")
    raise e

finally:
    # Finalizar a sessão Spark
    spark.stop()

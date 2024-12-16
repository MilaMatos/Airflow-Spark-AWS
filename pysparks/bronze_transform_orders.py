from pyspark.sql import SparkSession
 
spark = SparkSession.builder \
    .appName("BronzeTransform_Orders") \
    .config("spark.hadoop.fs.s3a.access.key", "SEU_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SEU_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.session.token", "SEU_SESSION_TOKEN") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

try:
    # Caminhos S3
    bucket_name = "cm-airflow-spark"
    landing_path = f"s3a://{bucket_name}/landing/orders.json"
    bronze_path = f"s3a://{bucket_name}/bronze/orders"

    # Ler os dados JSON da pasta landing no S3
    orders_bronze = spark.read.json(landing_path)

    # Salvar os dados transformados no formato Parquet na pasta bronze no S3
    orders_bronze.write.mode("overwrite").parquet(bronze_path)

except Exception as e:
        print(f"Erro: {e}")
        raise e


# Finalizar a sessão Spark
spark.stop()
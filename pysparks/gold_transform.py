from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Inicializar uma sessão Spark com configuração para S3
spark = SparkSession.builder \
    .appName("GoldTransform") \
    .config("spark.hadoop.fs.s3a.access.key", "SEU_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "SEU_SECRET_KEY") \
    .config("spark.hadoop.fs.s3a.session.token", "SEU_SESSION_TOKEN") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .getOrCreate()

try:
    # Caminhos S3 para leitura e escrita
    bucket_name = "cm-airflow-spark"
    silver_paths = {
        "customers": f"s3a://{bucket_name}/silver/customers",
        "orders": f"s3a://{bucket_name}/silver/orders",
        "order_items": f"s3a://{bucket_name}/silver/order_items"
    }
    gold_output_path = f"s3a://{bucket_name}/gold/orders_summary"

    # Ler os arquivos Parquet da camada "silver"
    customers_df = spark.read.parquet(silver_paths["customers"])
    orders_df = spark.read.parquet(silver_paths["orders"])
    order_items_df = spark.read.parquet(silver_paths["order_items"])

    # Juntando os DataFrames
    # Juntando customers com orders via id do cliente
    customers_orders_df = customers_df.join(orders_df, customers_df.id == orders_df.customer_id, "inner")

    # Juntando com order_items via id do pedido
    full_df = customers_orders_df.join(order_items_df, orders_df.id == order_items_df.order_id, "inner")

    # Agrupando os dados por cidade, estado e contando a quantidade de pedidos
    summary_df = full_df.groupBy('city', 'state') \
        .agg(
            F.countDistinct(orders_df.id).alias('qtd_de_pedidos'),  # Quantidade de pedidos por cidade e estado
            F.round(F.sum(order_items_df.subtotal), 2).alias('valor_total_pedidos')  # Valor total dos pedidos com 2 casas decimais
        )

    # Exibir o resultado, ordenando pelo valor total
    summary_df.select("city", "state", "qtd_de_pedidos", "valor_total_pedidos") \
        .orderBy(F.desc("valor_total_pedidos")) \
        .show(truncate=False)

    # Escrever os dados na camada "gold" em formato Parquet
    summary_df.write.mode("overwrite").parquet(gold_output_path)

    print("Transformação concluída e dados salvos na camada gold com sucesso!")

except Exception as e:
    print(f"Erro durante a execução: {e}")
    raise e

finally:
    # Parar a sessão Spark
    spark.stop()

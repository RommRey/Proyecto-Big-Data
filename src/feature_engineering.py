from pyspark.sql import Window
from pyspark.sql import functions as F

def apply_feature_engineering(df):
    """
    Aplica transformaciones de datos e ingeniería de características
    utilizando Window Functions en Apache Spark.
    """
    # Ventana de tiempo/secuencia agrupada por usuario (cuenta origen)
    window_user = Window.partitionBy("nameOrig").orderBy("step")

    df_featured = df \
        .withColumn("balance_delta_orig", F.round(F.col("oldbalanceOrg") - F.col("newbalanceOrig"), 2)) \
        .withColumn("balance_error_orig", F.round(F.abs(F.col("balance_delta_orig") - F.col("amount")), 2)) \
        .withColumn("tx_count_user", F.count("transaction_id").over(window_user)) \
        .withColumn("is_zero_balance_dest", 
                    F.when((F.col("oldbalanceDest") == 0) & (F.col("newbalanceDest") == 0), 1).otherwise(0))

    return df_featured
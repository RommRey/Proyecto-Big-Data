import time
import os
from src.spark_session import get_spark_session
from src.feature_engineering import apply_feature_engineering
from src.risk_scoring import calculate_risk_scores
from pyspark.sql import functions as F

def run_pipeline():
    print("🚀 [1/5] Iniciando Apache Spark Session...")
    spark = get_spark_session()
    start_time = time.time()

    TOTAL_RECORDS = 1000000
    print(f"⚡ [2/5] Generando {TOTAL_RECORDS:,} transacciones sintéticas...")
    
    base_df = spark.range(1, TOTAL_RECORDS + 1).toDF("transaction_id")
    
    df = base_df.withColumn("step", (F.rand() * 744).cast("integer") + 1) \
        .withColumn("type", F.element_at(
            F.array([F.lit("PAYMENT"), F.lit("TRANSFER"), F.lit("CASH_OUT"), F.lit("DEBIT"), F.lit("CASH_IN")]),
            (F.rand() * 5).cast("integer") + 1
        )) \
        .withColumn("amount", F.round(F.rand() * 10000 + 10, 2)) \
        .withColumn("nameOrig", F.concat(F.lit("C"), (F.rand() * 500000).cast("integer").cast("string"))) \
        .withColumn("oldbalanceOrg", F.round(F.rand() * 20000, 2)) \
        .withColumn("newbalanceOrig", F.when(F.col("oldbalanceOrg") - F.col("amount") < 0, 0)
                    .otherwise(F.round(F.col("oldbalanceOrg") - F.col("amount"), 2))) \
        .withColumn("nameDest", F.concat(
            F.when(F.col("type") == "PAYMENT", F.lit("M")).otherwise(F.lit("C")),
            (F.rand() * 500000).cast("integer").cast("string")
        )) \
        .withColumn("oldbalanceDest", F.round(F.rand() * 50000, 2)) \
        .withColumn("newbalanceDest", F.round(F.col("oldbalanceDest") + F.col("amount"), 2)) \
        .withColumn("isFraud", F.when(
            (F.col("type").isin("TRANSFER", "CASH_OUT")) & 
            ((F.col("amount") > 8000) | ((F.col("oldbalanceOrg") > 0) & (F.col("newbalanceOrig") == 0))), 1
        ).otherwise(0))

    print("🛠️ [3/5] Aplicando Feature Engineering en Spark...")
    df_featured = apply_feature_engineering(df)

    print("🧠 [4/5] Evaluando Risk Score y Categorías de Riesgo...")
    df_processed = calculate_risk_scores(df_featured)
    df_processed.cache()

    total_count = df_processed.count()
    elapsed = time.time() - start_time

    print("\n" + "="*60)
    print(f"✅ Pipeline completado en {elapsed:.2f} segundos")
    print(f"📊 Registros procesados: {total_count:,}")
    print("="*60 + "\n")

    # Muestra del resumen
    df_processed.groupBy("risk_category").count().show()

    print("📁 [5/5] Exportando muestra para Power BI...")
    os.makedirs("data/processed", exist_ok=True)
    
    # Exportamos una muestra representativa de 50,000 registros para Power BI Desktop
    pandas_df = df_processed.select(
        "transaction_id", "step", "type", "amount", "nameOrig",
        "oldbalanceOrg", "newbalanceOrig", "nameDest",
        "risk_score", "risk_category", "predicted_fraud", "isFraud"
    ).limit(50000).toPandas()

    output_path = "data/processed/fraud_detection_dashboard_data.csv"
    pandas_df.to_csv(output_path, index=False)
    print(f"✅ Archivo listo para Power BI en: {output_path}")

if __name__ == "__main__":
    run_pipeline()
from pyspark.sql import functions as F

def calculate_risk_scores(df_featured):
    """
    Calcula el puntaje de riesgo (Risk Score) de 0.00 a 1.00 para cada transacción
    y la clasifica en categorías de riesgo.
    """
    # Expresión ponderada basada en patrones de fraude
    risk_expression = (
        F.when(F.col("type").isin("TRANSFER", "CASH_OUT"), 0.25).otherwise(0.0) +
        F.when(F.col("amount") > 5000, 0.25).otherwise(F.col("amount") / 20000.0) +
        F.when(F.col("balance_error_orig") > 100, 0.25).otherwise(0.0) +
        F.when((F.col("oldbalanceOrg") > 0) & (F.col("newbalanceOrig") == 0), 0.25).otherwise(0.0)
    )

    df_scored = df_featured \
        .withColumn("raw_risk_score", F.least(F.lit(1.0), risk_expression)) \
        .withColumn("risk_score", F.round(F.col("raw_risk_score"), 2)) \
        .withColumn("risk_category", 
            F.when(F.col("risk_score") >= 0.70, "🔴 High Risk")
             .when(F.col("risk_score") >= 0.35, "🟡 Medium Risk")
             .otherwise("🟢 Low Risk")
        ) \
        .withColumn("predicted_fraud", F.when(F.col("risk_score") >= 0.70, 1).otherwise(0))

    return df_scored
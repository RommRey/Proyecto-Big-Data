import pytest
from pyspark.sql import SparkSession
from src.feature_engineering import apply_feature_engineering
from src.risk_scoring import calculate_risk_scores

@pytest.fixture(scope="module")
def spark():
    return SparkSession.builder \
        .appName("Test_Fraud_Analytics") \
        .master("local[1]") \
        .getOrCreate()

def test_feature_engineering_calculation(spark):
    data = [
        (1, 1, "TRANSFER", 5000.0, "C1001", 10000.0, 5000.0, "M999", 0.0, 5000.0)
    ]
    columns = ["transaction_id", "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest"]
    df = spark.createDataFrame(data, columns)
    
    df_featured = apply_feature_engineering(df)
    result = df_featured.collect()[0]
    
    assert "balance_delta_orig" in df_featured.columns
    assert result["balance_delta_orig"] == 5000.0

def test_risk_scoring_high_risk(spark):
    data = [
        (1, 1, "TRANSFER", 15000.0, "C1001", 15000.0, 0.0, "M999", 0.0, 15000.0, 0.0, 15000.0, 1, 0)
    ]
    columns = ["transaction_id", "step", "type", "amount", "nameOrig", "oldbalanceOrg", "newbalanceOrig", "nameDest", "oldbalanceDest", "newbalanceDest", "balance_delta_orig", "balance_error_orig", "tx_count_user", "is_zero_balance_dest"]
    df = spark.createDataFrame(data, columns)
    
    df_scored = calculate_risk_scores(df)
    result = df_scored.collect()[0]
    
    assert result["risk_category"] == "🔴 High Risk"
    assert result["predicted_fraud"] == 1
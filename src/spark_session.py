import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def get_spark_session(app_name="Proyecto_BigData_FraudAnalytics"):
    """
    Inicializa la sesión de Apache Spark en el entorno de Codespaces.
    """
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.driver.memory", "2g") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .getOrCreate()
    return spark

if __name__ == "__main__":
    spark = get_spark_session()
    print("🚀 Sesión de Apache Spark iniciada con éxito en Codespaces.")
    print("Versión de Spark:", spark.version)
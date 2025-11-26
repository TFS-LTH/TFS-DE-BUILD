try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql import types as T
    from pyspark.sql.window import Window as W

except ImportError as e:
    # Log or print the error for debugging
    print(f"Failed to import PySpark modules: {e}")
    # Assign dummy placeholders so that code referencing them doesn't immediately crash
    F = None
    T = None
    DataFrame = None
    SparkSession = None
    W = None

__all__ = ["F", "T", "DataFrame", "SparkSession", "W"]

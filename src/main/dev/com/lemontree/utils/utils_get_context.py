from pyspark.sql import SparkSession

try:
    from awsglue.context import GlueContext
except ImportError:
    # Mock GlueContext for local testing
    class GlueContext:
        def __init__(self, sc):
            self.spark_session = SparkSession.builder.appName("MockGlueApp").master("local[*]").getOrCreate()


def init_context(spark_session_name, config):
    spark_configs = config.get("spark_configs", {})
    sc = create_spark_session(spark_session_name, spark_configs).sparkContext
    glue_context = GlueContext(sc)
    spark_session = glue_context.spark_session
    spark_session.sparkContext.setLogLevel("OFF")

    print("Importing get_context")
    return glue_context, spark_session


def create_spark_session(app_name: str, configs: dict):
    builder = SparkSession.builder.appName(app_name)
    for key, value in configs.items():
        builder = builder.config(key, value)
    return builder.getOrCreate()

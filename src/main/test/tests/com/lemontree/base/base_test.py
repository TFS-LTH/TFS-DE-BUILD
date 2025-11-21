import pytest
from com.lemontree.configs.common_imports import F, T, DataFrame, SparkSession, W

# Try import GlueContext, else mock it for local
try:
    from awsglue.context import GlueContext
except ImportError:
    class GlueContext:
        def __init__(self, sc):
            self.spark_session = SparkSession.builder \
                .appName("MockGlueApp") \
                .master("local[*]") \
                .getOrCreate()

def init_context():
    spark_session = SparkSession.builder.appName("TestSuite").master("local[*]").getOrCreate()
    sc = spark_session.sparkContext
    glue_context = GlueContext(sc)
    spark_session.sparkContext.setLogLevel("OFF")
    print("Initialized Spark and Glue contexts")
    return glue_context, spark_session


class BaseTest:
    spark_session = None
    glue_context = None

    F = F
    T = T
    DataFrame = DataFrame
    W = W
    args= {}

    @pytest.fixture(scope="class", autouse=True)
    def setup_class(self, request):
        if BaseTest.spark_session is None or BaseTest.glue_context is None:
            BaseTest.glue_context, BaseTest.spark_session = init_context()

        self.spark_session = BaseTest.spark_session
        self.glue_context = BaseTest.glue_context
        print("Spark session started")

import sys
import logging
import redshift_connector
from com.lemontree.utils.utils_helper_methods import get_secrets_from_secret_manager
try:
    from awsglue.dynamicframe import DynamicFrame
except ImportError:
    DynamicFrame = None
    print("Warning: AWS Glue DynamicFrame not available. You might be running outside AWS Glue.")

# Logger setup
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_redshift_connection(secret_name='redshift_cdp_dwh', batchsize=10000, database=None):
    host, port, dbname, username, password, redshift_temp_dir = get_secrets_from_secret_manager(secret_name)
    if database:
        dbname = database

    conn = redshift_connector.connect(
        host=host,
        port=port,
        database=dbname,
        user=username,
        password=password
    )

    cursor = conn.cursor()
    cursor.execute("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED;")
    cursor.close()
    return conn

def write_to_redshift(glue_context, df, table_name, mode="append", secret_name='redshift_cdp_dwh',database = None):
    if DynamicFrame is None:
        raise ImportError("DynamicFrame is not available. Ensure you're running in AWS Glue environment.")

    logger.info("Writing to Redshift Table: %s with mode: %s", table_name, mode)

    host, port, dbname, username, password, redshift_temp_dir = get_secrets_from_secret_manager(secret_name)
    if database:
        dbname = database

    if mode == "overwrite":
        glue_context.write_dynamic_frame.from_options(
            frame = DynamicFrame.fromDF(df, glue_context, "test_temp"),
            connection_type="redshift",
            connection_options= {
                "url": f"jdbc:redshift://{host}:{port}/{dbname}",
                "dbtable": table_name,
                "user": username,
                "password": password,
                "redshiftTmpDir": redshift_temp_dir,
                "batchsize": "100000",
                "isolationLevel": "READ_COMMITTED",
                "preactions": f"TRUNCATE TABLE {table_name}"
            })
    else:
        glue_context.write_dynamic_frame.from_options(
            frame = DynamicFrame.fromDF(df, glue_context, "test_temp"),
            connection_type="redshift",
            connection_options= {
                "url": f"jdbc:redshift://{host}:{port}/{dbname}",
                "dbtable": table_name,
                "user": username,
                "password": password,
                "redshiftTmpDir": redshift_temp_dir,
                "batchsize": "100000",
                "isolationLevel": "READ_COMMITTED"
            })
    logger.info(f"Data written to Redshift table {table_name} successfully.")
    return None

def read_from_redshift(glue_context, table_name=None, query=None, secret_name='redshift_cdp_dwh', database=None):
    """
    Read from Redshift and return a dataframe. 
    """
    df = None
    host, port, dbname, username, password, redshift_temp_dir = get_secrets_from_secret_manager(secret_name)

    if database:
        dbname = database

    # Get Spark session from GlueContext
    spark = glue_context.spark_session

    if not (table_name or query):
        raise ValueError("Provide either table_name or query.")

    logger.info(f"Reading from Redshift. Table: {table_name}, Query: {query}")

    if query:
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:redshift://{host}:{port}/{dbname}") \
            .option("query", query) \
            .option("user", username) \
            .option("password", password) \
            .load()
    elif table_name:
        df = spark.read.format("jdbc") \
            .option("url", f"jdbc:redshift://{host}:{port}/{dbname}") \
            .option("dbtable", table_name) \
            .option("user", username) \
            .option("password", password) \
            .load()

    return df

def run_query_on_redshift(conn, query, commit=False, fetch=True):
    """
    Run a SQL query using an open connection.
    Forces transaction isolation level READ COMMITTED.
    - For SELECT: returns fetched rows.
    - For DML (INSERT/UPDATE/DELETE): returns affected row count.
    """
    try:
        with conn.cursor() as cursor:
            # cursor.execute("BEGIN;")
            # cursor.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED;")
            # logger.info("Transaction started with READ COMMITTED isolation.")

            cursor.execute(query)

            if cursor.description:  # SELECT or anything returning rows
                result = cursor.fetchall()
            else:  # DML (INSERT/UPDATE/DELETE)
                result = cursor.rowcount

        if commit:
            conn.commit()
            logger.info("Committed.")

        return result

    except Exception as e:
        logger.error(f"Error running query:\n{query}\nError: {e}")
        conn.rollback()
        logger.info("Rolled back.")
        sys.exit(1)

def align_schema_with_redshift(glue_context, df, existing_df):
    try:
        # Extract schema mapping from Redshift table
        existing_schema = {field.name.lower(): field.dataType for field in existing_df.schema.fields}
        logger.info(f"[INFO] Existing schema : {existing_schema}")

        # Standardize incoming DataFrame column names to lowercase
        df = df.selectExpr(*[f"`{col}` as {col.lower()}" for col in df.columns])

        from pyspark.sql.functions import col

        # Cast columns to match existing Redshift schema
        for col_name in df.columns:
            if col_name in existing_schema:
                df = df.withColumn(col_name, col(col_name).cast(existing_schema[col_name]))

        # Log final schema
        new_schema = {field.name: field.dataType for field in df.schema.fields}
        logger.info(f"[INFO] New aligned schema: {new_schema}")

        return df

    except Exception as e:
        logger.error(f"[ERROR] Schema alignment with Redshift failed: {e}")
        sys.exit(1)

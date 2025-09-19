import argparse
import yaml
import pkgutil
import shutil
import os
import boto3
import configparser
import uuid
import json
import logging
import pandas as pd
from datetime import date, timedelta

# Logger setup
MSG_FORMAT = '%(asctime)s %(levelname)s %(name)s: %(message)s'
DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(format=MSG_FORMAT, datefmt=DATETIME_FORMAT)
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def parse_cmd_line_args():
    parser = argparse.ArgumentParser()
    parser.add_argument('config', nargs='+', help="key=value arguments")
    args = parser.parse_args()
    result = {}
    for arg in args.config:
        if '=' not in arg:
            raise ValueError(f"Invalid argument: {arg}")
        key, val = arg.split('=', 1)
        result[key.strip()] = val.strip()
    return result

def read_parquet(spark, path):
    return spark.read.parquet(path)

def write_parquet(df, path, partitions):
    df.repartition(partitions).write.parquet(path)

def delete_directory(dir_path):
    if os.path.exists(dir_path) and os.path.isdir(dir_path):
        shutil.rmtree(dir_path)
        print(f"Removed directory: {dir_path}")
    else:
        print(f"Directory does not exist: {dir_path}")


def load_config_for_job(job_name: str) -> dict:
    full_config = load_config_from_package('configs/properties.yaml')
    job_config = full_config.get(job_name)
    if not job_config:
        raise ValueError(f"No config section found for job: {job_name}")

    # combining common_configs with job specific configs
    common_config = full_config['common_configs']
    merged_config = {**common_config, **job_config}
    print("Configurations loaded for the job: {}".format(job_name))
    print("Config section: {}".format(merged_config))
    return merged_config

def load_config_from_package(path_in_package: str) -> dict:
    print(f'Loading configuration from {path_in_package}')

    data = pkgutil.get_data('com.lemontree', path_in_package)
    if not data:
        print(f'Loaded error from {path_in_package}')
        raise FileNotFoundError(f"Could not find {path_in_package}")

    print(f'Config load complete from {path_in_package}')
    return yaml.safe_load(data.decode('utf-8'))

def get_secrets_from_secret_manager(secret_name, region='ap-south-1'):
    secrets_client = boto3.client("secretsmanager", region_name=region)
    secret = json.loads(secrets_client.get_secret_value(SecretId=secret_name)['SecretString'])
    return (
        secret.get('host',''),
        secret.get('port',''),
        secret.get('dbname'),
        secret.get('username',''),
        secret.get('password',''),
        secret.get('redshift_temp_dir','')
    )


def send_audit_log_message(msg: dict):
    expected_keys = ["glue_run_id", "job_name", "insert_record_count", "s3_file_modified_datetime", "update_record_count", "last_table_update_datetime", "table_name", "last_record_id", "src_sys_id"]
    for ky in expected_keys:
        if ky not in msg:
            raise Exception(f"Key {ky} is missing from message. If an empty value needs to be passed, then send null.")
    sqs = boto3.client("sqs")
    query_url = "https://sqs.ap-south-1.amazonaws.com/045101620276/cdp_audit_logs.fifo"
    try:
        sqs.send_message(
            QueueUrl=query_url,
            MessageBody=json.dumps(msg),
            MessageGroupId="audit-logs",  # required for FIFO queues
            MessageDeduplicationId=str(uuid.uuid4())  # ensures uniqueness
        )
        logger.info("Successfully sent message to SQS FIFO queue.")
    except Exception as e:
        logger.error("Exception occurred while sending data to SQS: %s", e)
        raise



def read_s3_data(glue_context, s3_input_path, file_format):
    # Get Spark session from GlueContext
    spark = glue_context.spark_session
    df = None

    if file_format == "parquet":
        df = spark.read.format("parquet") \
            .option("header", "true") \
            .load(s3_input_path)
        df.replace(["NaN", "nan", "@NULL@", "NULL"], None)

    elif file_format == "csv":
        df = spark.read.format("csv") \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .load(s3_input_path)
        df.replace(["NaN", "nan", "@NULL@", "NULL"], None)
    return df


def read_config_file(s3_client,configuration_file_bucket,configuration_file_key):
    config = configparser.RawConfigParser()
    # logger.info("Reading Configuration Data from Config file")
    obj = s3_client.get_object(Bucket=configuration_file_bucket, Key=configuration_file_key)
    config.read_string(obj['Body'].read().decode())
    return config


def read_from_sql_server(glue_context, table_name=None, query=None,secret_name='protel_production_credentials'):
    """
    Read from SQL Server using Glue DynamicFrame.
    Returns a DynamicFrame; convert to Spark DataFrame if required.
    """
    df = None
    host, port, dbname, username, password,_ = get_secrets_from_secret_manager(secret_name)
    if secret_name == 'credential_ssms':
        dbname = 'LTHDTTF'
    else:
        dbname = 'protel'

    # Build JDBC URL for SQL Server
    jdbc_url = f"jdbc:sqlserver://{host}:{port};databaseName={dbname}"

    if not (table_name or query):
        raise ValueError("Provide either table_name or query.")

    logger.info(f"Reading from SQL Server. Table: {table_name}, Query: {query}")

    # Get Spark session from GlueContext
    spark = glue_context.spark_session

    # Glue expects 'dbtable' for table or 'query' for SQL query
    if query:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user",username) \
            .option("password",password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()
    elif table_name:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user",username) \
            .option("password",password) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .load()

    return df

def read_from_mysql_server(glue_context, table_name=None, query=None, secret_name='mysql_rateping_cred'):
    """
    Read from SQL Server using Glue DynamicFrame.
    Returns a DynamicFrame; convert to Spark DataFrame if required.
    """
    df = None
    host, port, dbname, username, password,_ = get_secrets_from_secret_manager(secret_name)
    # Build JDBC URL for SQL Server
    jdbc_url = f"jdbc:mysql://{host}:{port}/{dbname}?zeroDateTimeBehavior=CONVERT_TO_NULL"

    if not (table_name or query):
        raise ValueError("Provide either table_name or query.")

    logger.info(f"Reading from SQL Server. Table: {table_name}, Query: {query}")

    # Get Spark session from GlueContext
    spark = glue_context.spark_session

    # Glue expects 'dbtable' for table or 'query' for SQL query
    if query:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("query", query) \
            .option("user",username) \
            .option("password",password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
    elif table_name:
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user",username) \
            .option("password",password) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .load()
    return df

def calculate_week_number_dynamic_year(current_date):
    if current_date is None:
        return None

    year = current_date.year
    start_date = date(year, 4, 1)  # April 1st

    # Find the first Sunday on or after April 1st
    start_day_of_week = start_date.isoweekday()  # Monday = 1, Sunday = 7
    offset_to_sunday = (7 - start_day_of_week + 1) % 7  # Days to next Sunday
    first_sunday = start_date + timedelta(days=offset_to_sunday)

    if current_date < first_sunday:
        return 0  # Before first valid week
    else:
        days_diff = (current_date - first_sunday).days
        week_num = (days_diff // 7) + 1  # +1 to make week number 1-based
        return week_num


def get_managed_hotels_from_tb(tb_file_path: str, hotel_codes: str = None) -> list:

    # Read the TB data for the month to get the hotel codes
    tb_full = read_tb_file(tb_file_path)
    if hotel_codes and hotel_codes.strip():
        managed_hotels = [i.strip() for i in hotel_codes.split(",")]
    else:
        managed_hotels = tb_full['Abbrevation'].dropna().tolist()

    print(f"Hotels considered for running: {managed_hotels}")

    return managed_hotels


def read_tb_file(tb_file_path: str):
    tb_full = pd.read_csv(tb_file_path, encoding='ISO-8859-1')
    tb_full['Abbrevation'] = tb_full['Abbrevation'].replace('LTHMBB', 'LTHMB2')
    tb_full['Abbrevation'] = tb_full['Abbrevation'].replace('LTHMB', 'LTHMB1')

    return tb_full
import os
from dotenv import load_dotenv

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    TableEnvironment,
    EnvironmentSettings,
)
# from pyflink.table.descriptors import FileSystem, Csv, Kafka, Json


# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
CSV_TOPIC_NAME = os.getenv("CSV_TOPIC_NAME")
JARS_FILE_PATH = os.getenv("JARS_FILE_PATH")
CSV_PATH = os.getenv("CSV_PATH")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

def get_jar_dependencies():
    files = os.listdir(JARS_FILE_PATH)
    jar_dependencies = []
    for file in files:
        if file.endswith('.jar'):
            jar_dependencies.append(f'file://{JARS_FILE_PATH}/{file}')

    jar_dependency_str = ';'.join(jar_dependencies)
    return jar_dependency_str


def csv_reader_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Enable checkpointing for exactly-once delivery
    # env.enable_checkpointing(60000)  # 60 seconds

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    jar_dependencies = get_jar_dependencies()
    t_env.get_config().set("pipeline.jars",
                           "{}".format(jar_dependencies)
                           )

    # print(f"PRINT JAR PATHS : {t_env.get_config().get('pipeline.jars', 'EMPTY')}")

    print(f"CSV_PATH : {CSV_PATH}")
    print(f"KAFKA_BOOTSTRAP_SERVERS : {KAFKA_BOOTSTRAP_SERVERS}")
    # conf = t_env.get_config().get_configuration()
    #
    # conf.set_string("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    # conf.set_string("fs.s3a.access.key", "<YOUR_ACCESS_KEY>")
    # conf.set_string("fs.s3a.secret.key", "<YOUR_SECRET_KEY>")
    # conf.set_string("fs.s3a.endpoint", "s3.amazonaws.com")
    # conf.set_string("fs.s3a.path.style.access", "false")  # true for MinIO


    # -------------------------------
    # 3️⃣ Define CSV Source Table on S3
    # -------------------------------
    t_env.execute_sql(f"""
                      CREATE TABLE csv_source
                      (
                          order_id STRING,
                          customer_name STRING,
                          product_name STRING,
                          order_status STRING
                      ) WITH (
                            'connector' = 'filesystem',
                            'path' = '{CSV_PATH}',
                            'format' = 'csv',
                            'csv.ignore-parse-errors' = 'true',
                            'csv.field-delimiter' = ',',
                            'source.monitor-interval' = '10'
                            )
                      """)

    # -------------------------------
    # 4️⃣ Define Kafka Sink Table
    # -------------------------------
    t_env.execute_sql(f"""
                      CREATE TABLE kafka_sink
                      (
                          order_id STRING,
                          customer_name STRING,
                          customer_first_name STRING,
                          product_name STRING,
                          order_status STRING
                      ) WITH (
                            'connector' = 'kafka',
                            'topic' = '{CSV_TOPIC_NAME}',
                            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
                            'format' = 'json',
                            'key.format' = 'raw',
                            'key.raw.charset' = 'UTF-8',
                            'key.fields' = 'order_id'
                            )
                      """)

    # -------------------------------
    # 5️⃣ Continuous Streaming from CSV → Kafka
    # -------------------------------
    t_env.execute_sql("""
                      INSERT INTO kafka_sink
                      SELECT
                          order_id,
                          customer_name,
                          SPLIT_INDEX(customer_name, ' ', 0) AS customer_first_name,
                          product_name,
                          order_status
                      FROM csv_source
                      """)



if __name__ == "__main__":
    csv_reader_flink_job()
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
POSTGRES_HOST=os.getenv("POSTGRES_HOST")

def get_jar_dependencies():
    files = os.listdir(JARS_FILE_PATH)
    jar_dependencies = []
    for file in files:
        if file.endswith('.jar'):
            jar_dependencies.append(f'file://{JARS_FILE_PATH}/{file}')

    jar_dependency_str = ';'.join(jar_dependencies)
    return jar_dependency_str


def jdbc_flink_job():
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
                      CREATE TABLE orders_jdbc (
                        order_id STRING,
                        product_id STRING,
                        quantity INT,
                        order_ts TIMESTAMP(3)
                        ) WITH (
                           'connector' = 'jdbc',
                           'url' = 'jdbc:postgresql://{POSTGRES_HOST}:5432/postgres',
                           'table-name' = 'public.orders_jdbc',
                           'username' = 'postgres',
                           'password' = 'kp|$XRh#39$rucJ$.8Wz>h<ewP|>'
                        )
                      """)


    result = t_env.execute_sql(f"""
                      SELECT * FROM orders_jdbc
                      """)

    result.print()


if __name__ == "__main__":
    jdbc_flink_job()
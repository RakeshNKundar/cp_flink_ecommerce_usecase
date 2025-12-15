import os
from dotenv import load_dotenv

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import (
    StreamTableEnvironment,
    EnvironmentSettings
)

load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC")
PRODUCTS_TOPIC = os.getenv("PRODUCTS_TOPIC")
ENRICHED_ORDERS_TOPIC = os.getenv("ENRICHED_ORDERS_TOPIC")
PRODUCT_COUNTS_TOPIC = os.getenv("PRODUCT_COUNTS_TOPIC")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", 5))
JARS_FILE_PATH = os.getenv("JARS_FILE_PATH")
POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_SCHEMA = os.getenv("POSTGRES_SCHEMA")
POSTGRES_TABLE = os.getenv("POSTGRES_TABLE")

CURRENT_WORKING_DIRECTORY = os.getcwd()
CSV_PATH = f"{CURRENT_WORKING_DIRECTORY}/{'csv_files'}"
os.makedirs('csv_files', exist_ok=True)


def get_jar_dependencies():
    files = os.listdir(JARS_FILE_PATH)
    # print(f"JARS List : {files}")
    jars = [f"file://{CURRENT_WORKING_DIRECTORY}/{JARS_FILE_PATH}/{file}"
            for file in files if file.endswith(".jar")]
    return ";".join(jars)

def postgres_cdc_flink_job():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = StreamTableEnvironment.create(
        env, environment_settings=env_settings
    )

    t_env.get_config().set("pipeline.jars", get_jar_dependencies())

    # print(f"PRINT JAR PATHS : {t_env.get_config().get('pipeline.jars', 'EMPTY')}")

    t_env.execute_sql(f"""
          CREATE TABLE orders_csv
          (
              order_id STRING,
              product_id STRING,
              quantity INT,
              order_ts TIMESTAMP(3)
          ) WITH (
                'connector' = 'filesystem',
                'path' = '{CSV_PATH}',
                'format' = 'csv',
                'csv.ignore-parse-errors' = 'true',
                'csv.field-delimiter' = ',',
                'source.monitor-interval' = '10',
                'sink.parallelism' = '1'
                )
          """)

    t_env.execute_sql(f"""
        CREATE TABLE orders (
            order_id STRING,
            product_id STRING,
            quantity INT,
            order_ts TIMESTAMP(3),
            WATERMARK FOR order_ts AS order_ts - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{ORDERS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-group-{ORDERS_TOPIC}',
            'value.format' = 'avro-confluent',
            'properties.schema.registry.url' = '{SCHEMA_REGISTRY_URL}',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'scan.startup.mode' = 'earliest-offset',
            'key.format' = 'raw',
            'key.raw.charset' = 'UTF-8',
            'key.fields' = 'product_id'
        )
        """)


    t_env.execute_sql(f"""
          CREATE TABLE products_source
          (
            product_id STRING,
            product_name STRING,
            stock INT,
            last_updated_ts TIMESTAMP(3),
            PRIMARY KEY (product_id) NOT ENFORCED
            ) WITH (
              'connector' = 'postgres-cdc',
              'hostname' = '{POSTGRES_HOST}',
              'port' = '5432',
              'username' = '{POSTGRES_USERNAME}',
              'password' = '{POSTGRES_PASSWORD}',
              'database-name' = '{POSTGRES_DATABASE}',
              'schema-name' = '{POSTGRES_SCHEMA}',
              'table-name' = '{POSTGRES_TABLE}',
              'slot.name' = 'flink',
              'changelog-mode' = 'upsert',
              'scan.incremental.snapshot.enabled' = 'true',
              'scan.startup.mode' = 'initial',
              'decoding.plugin.name' = 'pgoutput',
              'heartbeat.interval.ms' = '30'
            )
         """)


    t_env.execute_sql(f"""
            CREATE TABLE products (
                product_id STRING,
                product_name STRING,
                stock INT,
                last_updated_ts TIMESTAMP(3),
                PRIMARY KEY (product_id) NOT ENFORCED,
                WATERMARK FOR last_updated_ts AS last_updated_ts - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'upsert-kafka',
                'topic' = '{PRODUCTS_TOPIC}',
                'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
                'properties.group.id' = 'flink-group-{PRODUCTS_TOPIC}',
                'value.format' = 'avro-confluent',
                'properties.schema.registry.url' = '{SCHEMA_REGISTRY_URL}',
                'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
                'key.format' = 'raw',
                'key.raw.charset' = 'UTF-8',
                'properties.allow.auto.create.topics' = 'true'
            )
            """)

    t_env.execute_sql(f"""
            CREATE TABLE enriched_orders (
                order_id STRING,
                product_id STRING,
                product_name STRING,
                quantity INT,
                order_ts TIMESTAMP(3),
                WATERMARK FOR order_ts AS order_ts - INTERVAL '1' SECOND
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{ENRICHED_ORDERS_TOPIC}',
                'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
                'properties.group.id' = 'flink-group-{ENRICHED_ORDERS_TOPIC}',
                'value.format' = 'avro-confluent',
                'properties.schema.registry.url' = '{SCHEMA_REGISTRY_URL}',
                'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
                'key.format' = 'raw',
                'key.raw.charset' = 'UTF-8',
                'properties.allow.auto.create.topics' = 'true',
                'key.fields' = 'product_id',
                'scan.startup.mode' = 'earliest-offset'
            )
        """)

    t_env.execute_sql(f"""
            CREATE TABLE product_window_counts (
                product_id STRING,
                order_count BIGINT,
                window_start TIMESTAMP(3),
                window_end TIMESTAMP(3)
            ) WITH (
                'connector' = 'kafka',
                'topic' = '{PRODUCT_COUNTS_TOPIC}',
                'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
                'properties.group.id' = 'flink-group-{ENRICHED_ORDERS_TOPIC}',
                'value.format' = 'avro-confluent',
                'properties.schema.registry.url' = '{SCHEMA_REGISTRY_URL}',
                'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
                'key.format' = 'raw',
                'key.raw.charset' = 'UTF-8',
                'properties.allow.auto.create.topics' = 'true',
                'key.fields' = 'product_id',
                'scan.startup.mode' = 'earliest-offset'
            )
            """)

    statement_set = t_env.create_statement_set()

    statement_set.add_insert_sql("""
             INSERT INTO products
             SELECT product_id,
                    product_name,
                    stock,
                    last_updated_ts
             FROM products_source
             """)

    statement_set.add_insert_sql("""
             INSERT INTO orders
             SELECT order_id,
                    product_id,
                    quantity,
                    order_ts
             FROM orders_csv
             """)

    statement_set.add_insert_sql("""
             INSERT INTO enriched_orders
             SELECT o.order_id,
                    o.product_id,
                    p.product_name,
                    o.quantity,
                    o.order_ts
             FROM orders AS o
             JOIN products FOR SYSTEM_TIME AS OF o.order_ts AS p
             ON o.product_id = p.product_id
             """)

    statement_set.add_insert_sql(f"""
            INSERT INTO product_window_counts
            SELECT
                product_id,
                COUNT(order_id) AS order_count,
                TUMBLE_START(order_ts, INTERVAL '{WINDOW_MINUTES}' MINUTE) AS window_start,
                TUMBLE_END(order_ts, INTERVAL '{WINDOW_MINUTES}' MINUTE) AS window_end
            FROM enriched_orders
            GROUP BY product_id, TUMBLE(order_ts, INTERVAL '{WINDOW_MINUTES}' MINUTE)
        """)

    # Execute all inserts together
    statement_set.execute()

if __name__ == "__main__":
    postgres_cdc_flink_job()

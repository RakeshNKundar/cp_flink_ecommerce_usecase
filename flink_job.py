import os
from dotenv import load_dotenv
import logging
import sys

from pyflink.table import (EnvironmentSettings, TableEnvironment)

# from pyflink.table.catalog import GenericInMemoryCatalog

# Load environment variables
load_dotenv()

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
ORDERS_TOPIC = os.getenv("ORDERS_TOPIC")
PRODUCTS_TOPIC = os.getenv("PRODUCTS_TOPIC")
PRODUCTS_MATERIALIZED_TOPIC = os.getenv("PRODUCTS_MATERIALIZED_TOPIC")
ENRICHED_ORDERS_TOPIC = os.getenv("ENRICHED_ORDERS_TOPIC")
PRODUCT_COUNTS_TOPIC = os.getenv("PRODUCT_COUNTS_TOPIC")
WINDOW_MINUTES = int(os.getenv("WINDOW_MINUTES", 5))
JARS_FILE_PATH = os.getenv("JARS_FILE_PATH")

def get_jar_dependencies():
    files = os.listdir(JARS_FILE_PATH)
    jar_dependencies = []
    for file in files:
        if file.endswith('.jar'):
            jar_dependencies.append(f'file://{JARS_FILE_PATH}/{file}')

    jar_dependency_str = ';'.join(jar_dependencies)
    return jar_dependency_str


def flink_avro_job():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")

    env_settings = EnvironmentSettings.in_streaming_mode()
    t_env = TableEnvironment.create(env_settings)

    # # Create a catalog named 'kafka_catalog' with a database 'kafka_db'
    # catalog = GenericInMemoryCatalog("kafka_catalog", default_database="kafka_db")
    # t_env.register_catalog("kafka_catalog", catalog)

    # # Switch to your custom catalog and database
    # t_env.use_catalog("kafka_catalog")
    # t_env.use_database("kafka_db")

    jar_dependencies = get_jar_dependencies()
    t_env.get_config().set("pipeline.jars",
                           "{}".format(jar_dependencies)
                           )
    print(f"PRINT JAR PATHS : {t_env.get_config().get('pipeline.jars', 'EMPTY')}")
    # -----------------------------
    # Source: Orders (Avro)
    # -----------------------------

    t_env.execute_sql(f"""
        CREATE TABLE orders (
            order_id STRING,
            product_id STRING,
            quantity INT,
            order_ts TIMESTAMP(3),
            WATERMARK FOR order_ts AS order_ts - INTERVAL '2' SECOND
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

    # -----------------------------
    # Source: Products (Avro)
    # -----------------------------

    t_env.execute_sql(f"""
        CREATE TABLE products (
            product_id STRING,
            product_name STRING,
            stock INT,
            last_updated_ts TIMESTAMP(3),
            WATERMARK FOR last_updated_ts AS last_updated_ts - INTERVAL '2' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{PRODUCTS_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-group-{PRODUCTS_TOPIC}',
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
        CREATE TABLE products_materialized (
            product_id STRING,
            product_name STRING,
            stock INT,
            last_updated_ts TIMESTAMP(3),
            WATERMARK FOR last_updated_ts AS last_updated_ts - INTERVAL '2' SECOND,
            PRIMARY KEY (product_id) NOT ENFORCED
        ) WITH (
            'connector' = 'upsert-kafka',
            'topic' = '{PRODUCTS_MATERIALIZED_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BOOTSTRAP_SERVERS}',
            'properties.group.id' = 'flink-group-{PRODUCTS_MATERIALIZED_TOPIC}',
            'value.format' = 'avro-confluent',
            'properties.schema.registry.url' = '{SCHEMA_REGISTRY_URL}',
            'value.avro-confluent.url' = '{SCHEMA_REGISTRY_URL}',
            'key.format' = 'raw',
            'key.raw.charset' = 'UTF-8',
            'properties.allow.auto.create.topics' = 'true'
        )
        """)



    # # -----------------------------
    # # Source: Enriched_orders (Avro)
    # # -----------------------------

    t_env.execute_sql(f"""
        CREATE TABLE enriched_orders (
            order_id STRING,
            product_id STRING,
            product_name STRING,
            quantity INT,
            order_ts TIMESTAMP(3),
            WATERMARK FOR order_ts AS order_ts - INTERVAL '2' SECOND
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

    # Create Windowed Counts Sink Table
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

    # Add multiple inserts
    statement_set.add_insert_sql("""
                                 INSERT INTO products_materialized
                                 SELECT product_id, product_name, stock, last_updated_ts
                                 FROM products
                                 """)

    statement_set.add_insert_sql("""
                                 INSERT INTO enriched_orders
                                 SELECT o.order_id,
                                        o.product_id,
                                        p.product_name,
                                        o.quantity,
                                        o.order_ts
                                 FROM orders AS o
                                 JOIN products_materialized FOR SYSTEM_TIME AS OF o.order_ts AS p
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


if __name__ == '__main__':
    flink_avro_job()

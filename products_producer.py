import time
import os
import random
from datetime import datetime

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.avro import AvroSerializer

from faker import Faker
from faker_commerce import Provider as CommerceProvider
from dotenv import load_dotenv

import psycopg2

from postgres_cdc_flink_job import POSTGRES_HOST

# Load environment variables
load_dotenv()

from confluent_kafka.avro import AvroProducer
from confluent_kafka import avro


# ----------------------------------------------------------------------
# Configuration: UPDATE these for your environment
# ----------------------------------------------------------------------

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
TOPIC = "products"
POSTGRES_HOST = str(os.getenv("POSTGRES_HOST"))
POSTGRES_USER = str(os.getenv("POSTGRES_USERNAME"))
POSTGRES_PASSWORD = str(os.getenv("POSTGRES_PASSWORD"))

fake = Faker()
Faker.seed(42)
fake.add_provider(CommerceProvider)


# ----------------------------------------------------------------------
# Mock data generator
# ----------------------------------------------------------------------

def generate_order():
    return {
        "product_id": f"product-{random.randint(1, 50)}",
        "product_name": fake.ecommerce_name(),
        "stock": random.randint(100, 1000),
        "last_updated_ts": datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]
    }

def produce_products_postgres():

    print("🚀 Producing messages to Postgres Products table ... ctrl+c to stop")

    try:
        # Connect to PostgreSQL
        connection = {
            "host": POSTGRES_HOST,
            "database": "postgres",
            "user": POSTGRES_USER,
            "password": POSTGRES_PASSWORD
        }

        while True:
            order = generate_order()

            cols = ", ".join(order.keys())
            placeholders = ", ".join(["%s"] * len(order))
            values = list(order.values())

            # Build SET part: col = EXCLUDED.col
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in order.keys()])

            query = f"""
                    INSERT INTO products ({cols})
                    VALUES ({placeholders})
                    ON CONFLICT (product_id)
                    DO UPDATE SET {set_clause}
                """

            # Execute UPSERT
            with psycopg2.connect(**connection) as conn:
                with conn.cursor() as cur:
                    cur.execute(query, values)
                    conn.commit()

            print(f"Produced {order['product_id']} to table products")

    except KeyboardInterrupt:
        print("\nStopping…")

    finally:
        pass


if __name__ == "__main__":
    produce_products_postgres()
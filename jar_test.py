from pyflink.java_gateway import get_gateway
from pyflink.table import TableEnvironment, EnvironmentSettings

# Initialize TableEnvironment
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# Optional: set pipeline.jars (your CDC connector)
# t_env.get_config().set(
#     "pipeline.jars",
#     [
#         "/Users/rakeshkundar/jars/flink-connector-postgres-cdc-3.5.0.jar",
#         "/Users/rakeshkundar/jars/flink-cdc-runtime-3.5.0.jar",
#         "/Users/rakeshkundar/jars/debezium-connector-postgres-1.9.8.Final.jar",
#         "/Users/rakeshkundar/jars/postgresql-42.6.0.jar",
#     ]
# )

# Get JVM gateway
gateway = get_gateway()
classloader = gateway.jvm.Thread.currentThread().getContextClassLoader()

# Get all URLs from the classloader
urls = classloader.getURLs()

print("=== All JARs visible to JVM ===")
for url in urls:
    print(url.getFile())
print("=== End of JAR list ===")

# Optional: check if a specific class is visible
try:
    gateway.jvm.org.apache.flink.streaming.api.functions.source.SourceFunction
    print("SourceFunction is visible!")
except Exception:
    print("SourceFunction NOT visible!")

try:
    gateway.jvm.org.apache.flink.cdc.connectors.postgres.table.PostgreSQLTableFactory
    print("PostgreSQLTableFactory is visible!")
except Exception:
    print("PostgreSQLTableFactory NOT visible!")

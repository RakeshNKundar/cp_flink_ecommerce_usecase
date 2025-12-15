FROM debian:latest AS builder

# Install dependencies first
RUN set -ex; \
  apt-get update; \
  apt-get -y install gcc default-jdk; \
  rm -rf /var/lib/apt/lists/*

# Set the correct JAVA_HOME
ENV JAVA_HOME=/usr/lib/jvm/default-java

# Setup python environment with uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /bin/

# Install python in the app folder so we can port it over
ENV UV_PYTHON_INSTALL_DIR=/opt/flink/pyflink/.uv

# Create the virtual environment
RUN mkdir -p /opt/flink/pyflink
WORKDIR /opt/flink/pyflink
RUN uv venv --python 3.11 .venv

# Copy the project and install dependencies. This demo only requires cp-pyflink,
# but you can install dependencies the way you prefer. We suggest building a project
# that can be setup by running `uv sync`.
# Make sure to install the correct version of cp-pyflink
COPY pyproject.toml uv.lock .env ./
COPY flink_job.py ./

RUN uv sync

# Build the final image, copy the python project and virtualenv in /opt/flink/pyflink
FROM confluentinc/cp-flink:2.1.0-cp1

COPY --from=builder --chown=flink:flink /opt/flink/pyflink/ /opt/flink/pyflink/

# Set working directory
WORKDIR /opt/flink/pyflink

# Download Flink Kafka connector JARs
USER root

RUN mkdir -p /opt/flink/jars

COPY ./jars /opt/flink/jars
# Make sure flink user owns jars
RUN chown -R flink:flink /opt/flink/jars

# Set FLINK_CLASSPATH for Kafka connector JARs
ENV FLINK_CLASSPATH=/opt/flink/jars/*

USER flink
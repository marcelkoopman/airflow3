FROM apache/airflow:3.0.1

USER root

# Install system dependencies if needed
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        build-essential \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME for PySpark - using the correct ARM64 path
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$PATH:$JAVA_HOME/bin

# Switch back to airflow user
USER airflow

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# Copy any custom scripts or configurations
# COPY scripts/ /opt/airflow/scripts/
COPY config/ /opt/airflow/config/
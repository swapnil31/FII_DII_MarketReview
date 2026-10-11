FROM apache/airflow:2.10.5-python3.11

# Java is required by Spark. Installing system packages needs root.
USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
 && apt-get clean \
 && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Back to the airflow user for Python packages.
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir --default-timeout=120 --retries 10 -r /requirements.txt
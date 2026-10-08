FROM apache/spark:3.5.1

USER root

# Install requests for bulk Elasticsearch REST communication
RUN pip install --no-cache-dir requests==2.31.0

# Ensure writable Ivy cache and temporary working directories
ENV HOME=/tmp
RUN mkdir -p /tmp/.ivy2 /tmp/cloudpulse-checkpoints && chmod -R 777 /tmp

WORKDIR /opt/spark-apps
COPY metrics_job.py /opt/spark-apps/metrics_job.py

USER 185

ENTRYPOINT ["/opt/spark/bin/spark-submit", \
    "--master", "spark://spark-master:7077", \
    "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
    "--conf", "spark.driver.extraJavaOptions=-Divy.home=/tmp/.ivy2", \
    "/opt/spark-apps/metrics_job.py", \
    "--kafka", "kafka:9092", \
    "--es-host", "elasticsearch:9200"]

FROM apache/spark:3.5.1

USER root
RUN pip install --no-cache-dir requests
COPY vote_aggregator.py /opt/spark/vote_aggregator.py

# HOME=/tmp sidesteps the apache/spark image's non-root user having an
# unwritable home directory, which otherwise breaks Ivy's dependency
# cache when spark-submit resolves the Kafka connector package.
ENV HOME=/tmp

CMD ["/opt/spark/bin/spark-submit", \
     "--master", "spark://spark-master:7077", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1", \
     "/opt/spark/vote_aggregator.py", \
     "--kafka", "kafka:9092", \
     "--es-host", "elasticsearch:9200"]

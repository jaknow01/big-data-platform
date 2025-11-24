FROM bitnami/spark:3.4.1

USER root

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.4.1/spark-sql-kafka-0-10_2.13-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.2.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar

      RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY 04_spark_streaming_job .

CMD ["spark-submit", "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.1,io.delta:delta-spark_2.13:4.0.0,org.apache.hadoop:hadoop-aws:3.3.4", "04_spark_streaming_job"]
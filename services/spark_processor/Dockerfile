FROM bitnami/spark:3.4.1

USER root

RUN mkdir -p /opt/bitnami/spark/jars && \
    curl -L -o /opt/bitnami/spark/jars/spark-sql-kafka-0-10_2.13-3.4.1.jar \
      https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/3.4.1/spark-sql-kafka-0-10_2.13-3.4.1.jar && \
    curl -L -o /opt/bitnami/spark/jars/kafka-clients-3.2.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.2.0/kafka-clients-3.2.0.jar

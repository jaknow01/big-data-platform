FROM debezium/connect:2.4.2.Final

USER root

# Pobierz i zainstaluj Confluent Schema Registry konwertery
RUN curl -L -o /tmp/confluent-avro-converter.zip \
    "https://packages.confluent.io/maven/io/confluent/kafka-connect-avro-converter/7.4.0/kafka-connect-avro-converter-7.4.0.jar" && \
    mkdir -p /kafka/connect/confluent-avro-converter && \
    mv /tmp/confluent-avro-converter.zip /kafka/connect/confluent-avro-converter/kafka-connect-avro-converter-7.4.0.jar

# Pobierz dodatkowe zależności dla Avro
RUN curl -L -o /kafka/connect/confluent-avro-converter/kafka-avro-serializer-7.4.0.jar \
    "https://packages.confluent.io/maven/io/confluent/kafka-avro-serializer/7.4.0/kafka-avro-serializer-7.4.0.jar" && \
    curl -L -o /kafka/connect/confluent-avro-converter/kafka-schema-serializer-7.4.0.jar \
    "https://packages.confluent.io/maven/io/confluent/kafka-schema-serializer/7.4.0/kafka-schema-serializer-7.4.0.jar" && \
    curl -L -o /kafka/connect/confluent-avro-converter/common-config-7.4.0.jar \
    "https://packages.confluent.io/maven/io/confluent/common-config/7.4.0/common-config-7.4.0.jar" && \
    curl -L -o /kafka/connect/confluent-avro-converter/common-utils-7.4.0.jar \
    "https://packages.confluent.io/maven/io/confluent/common-utils/7.4.0/common-utils-7.4.0.jar"

# Wróć do użytkownika kafka
USER kafka
FROM apache/spark:3.4.1-python3

USER root

# Instalacja Pythona i zależności systemowych
RUN apt-get update && apt-get install -y python3-pip && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY build/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scripts .

CMD ["/opt/spark/bin/spark-submit", \
     "--packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-aws:3.3.4", \
     "spark_streaming_job.py"]
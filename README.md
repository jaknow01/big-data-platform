# Big Data Platform in Docker Containers – Semester Project

A big data analysis platform built from open-source components based on [link](https://github.com/jaknow01/mini-data-platform) but with extended capabilities.
Most of the services now run inside Airflow. There are three DAGs that control the flow of data.

The first one initializes Postgres database, ingests data, sets up Debezium connector that sends the data into Kafka. Kafka then writes it into Minio.
The second is all about data processing with Spark through bronze and silver layer. At the end a report about the state of the data is generated with great_expectations library.
Third DAG trains a simple linear regression model which is then saved to mlflow server.




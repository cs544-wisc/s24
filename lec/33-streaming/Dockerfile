FROM ubuntu:22.04

# apt
RUN apt-get update; apt-get install -y wget curl openjdk-11-jdk python3-pip net-tools lsof nano
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# Jupyter
RUN pip3 install jupyterlab==4.0.3 pandas==2.1.1 matplotlib==3.8.0 kafka-python==2.0.2 \
    grpcio==1.58.0 grpcio-tools==1.58.0

# Kafka (see https://kafka.apache.org/quickstart, KRaft config)
RUN wget https://downloads.apache.org/kafka/3.6.2/kafka_2.12-3.6.2.tgz && tar -xf kafka_2.12-3.6.2.tgz && rm kafka_2.12-3.6.2.tgz

# from bin/kafka-storage.sh random-uuid
ENV KAFKA_CLUSTER_ID=dCHffFWYTCKWXiesmJMN9w

# SPARK
RUN wget https://dlcdn.apache.org/spark/spark-3.5.1/spark-3.5.1-bin-hadoop3.tgz && tar -xf spark-3.5.1-bin-hadoop3.tgz && rm spark-3.5.1-bin-hadoop3.tgz
RUN pip3 install pyspark==3.5.1

CMD sh -c "cd /kafka_2.12-3.6.2 && \
    bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties && \
    bin/kafka-server-start.sh config/kraft/server.properties"

FROM ubuntu:20.04

ENV DEBIAN_FRONTEND=noninteractive
ENV TRINO_VERSION=437


RUN apt-get update && apt-get install -y curl
RUN curl -L https://repo1.maven.org/maven2/io/trino/trino-cli/${TRINO_VERSION}/trino-cli-${TRINO_VERSION}-executable.jar \
    -o /usr/bin/trino && chmod +x /usr/bin/trino
RUN apt-get update && apt-get install -y \
    openjdk-21-jdk wget curl tar unzip gnupg python3 && \
    ln -s /usr/bin/python3 /usr/bin/python && \

    wget https://repo1.maven.org/maven2/io/trino/trino-server/${TRINO_VERSION}/trino-server-${TRINO_VERSION}.tar.gz && \
    tar -xzf trino-server-${TRINO_VERSION}.tar.gz && \
    mv trino-server-${TRINO_VERSION} /opt/trino && \
    rm trino-server-${TRINO_VERSION}.tar.gz && \

    wget https://archive.apache.org/dist/hadoop/common/hadoop-2.7.4/hadoop-2.7.4.tar.gz && \
    tar -xzf hadoop-2.7.4.tar.gz && \
    mv hadoop-2.7.4 /opt/hadoop && \
    rm hadoop-2.7.4.tar.gz && \
    mkdir -p /etc/hadoop/conf

# COPY trino/etc /opt/trino/etc
# COPY trino/catalog /opt/trino/etc/catalog
# COPY hadoop-config/core-site.xml /etc/hadoop/conf/core-site.xml

# ENV để Trino plugin hive nhận ra cấu hình Hadoop
ENV HADOOP_CONF_DIR=/etc/hadoop/conf
ENV PATH=$PATH:/opt/hadoop/bin:/opt/trino/bin

EXPOSE 8080

CMD ["/opt/trino/bin/launcher", "run"]

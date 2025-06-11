# ARG AIRFLOW_IMAGE_NAME
# FROM ${AIRFLOW_IMAGE_NAME}
# # ----- Root for installing extra packages -----
# USER root

# RUN apt-get update -qq \
#  && apt-get install -yqq vim curl

# ENV JAVA_HOME=/opt/java
# RUN DOWNLOAD_URL="https://download.java.net/java/GA/jdk11/9/GPL/openjdk-11.0.2_linux-x64_bin.tar.gz" \
#     && mkdir -p "${JAVA_HOME}" \
#     && curl -fL "${DOWNLOAD_URL}" | tar -xz -C "${JAVA_HOME}" --strip-components=1 \
#     && echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile \
#     && echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile \
#     && ln -s ${JAVA_HOME}/bin/java /usr/bin/java \
#     && java --version

# COPY ./requirements.txt /opt/airflow/requirements.txt
# RUN python -m pip install --upgrade pip \
#  && python -m pip install --no-cache-dir -r /opt/airflow/requirements.txt
 
# RUN echo $PATH && which airflow
# RUN airflow version || true
# USER ${AIRFLOW_UID}


ARG AIRFLOW_IMAGE_NAME
FROM ${AIRFLOW_IMAGE_NAME}

USER root

RUN apt-get update -qq \
 && apt-get install -yqq vim curl ca-certificates

ENV JAVA_HOME=/opt/java

RUN DOWNLOAD_URL="https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u392-b08/OpenJDK8U-jdk_x64_linux_hotspot_8u392b08.tar.gz" \
    && mkdir -p "${JAVA_HOME}" \
    && curl -fL "${DOWNLOAD_URL}" | tar -xz -C "${JAVA_HOME}" --strip-components=1 \
    && echo "export JAVA_HOME=${JAVA_HOME}" >> /etc/profile \
    && echo "export PATH=\$JAVA_HOME/bin:\$PATH" >> /etc/profile \
    && ln -s ${JAVA_HOME}/bin/java /usr/bin/java \
    && java -version

COPY ./requirements.txt /opt/airflow/requirements.txt

RUN python -m pip install --upgrade pip \
 && python -m pip install --no-cache-dir -r /opt/airflow/requirements.txt

RUN echo $PATH && which airflow
RUN airflow version || true

USER ${AIRFLOW_UID}

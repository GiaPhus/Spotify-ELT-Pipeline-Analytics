#FROM python:3.9.16-slim
FROM python:3.11-slim

SHELL ["/bin/bash", "-o", "pipefail", "-c"]
USER root

ARG openjdk_version="17"

RUN apt-get update --yes && \
    apt-get install --yes curl "openjdk-${openjdk_version}-jre-headless" ca-certificates-java procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*


ENV JAVA_HOME /usr/lib/jvm/java-17-openjdk-arm64
RUN export JAVA_HOME

USER root
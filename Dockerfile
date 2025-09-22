FROM quay.io/astronomer/astro-runtime:12.4.0

USER root

RUN apt update && \
    apt install -y openjdk-17-jdk ant curl unzip && \
    apt clean

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-arm64
ENV PATH=$JAVA_HOME/bin:$PATH

RUN pip install --upgrade pip
RUN pip install \
    pandas \
    duckdb \
    pytrends \
    elasticsearch \
    requests

USER astro

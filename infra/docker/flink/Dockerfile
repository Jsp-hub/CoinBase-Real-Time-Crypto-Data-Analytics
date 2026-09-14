FROM flink:1.20.1-scala_2.12-java17

USER root

COPY pipelines/flink/requirements.txt /opt/project/requirements.txt

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 python3-pip \
    && ln -sf /usr/bin/python3 /usr/local/bin/python \
    && rm -rf /var/lib/apt/lists/* \
    && pip3 install --no-cache-dir -r /opt/project/requirements.txt \
    && curl --fail --location --silent --show-error \
      --output /opt/flink/lib/flink-connector-kafka-3.3.0-1.20.jar \
      https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar \
    && curl --fail --location --silent --show-error \
      --output /opt/flink/lib/kafka-clients-3.7.0.jar \
      https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.7.0/kafka-clients-3.7.0.jar

COPY pipelines/flink/candle_analytics.py /opt/project/candle_analytics.py

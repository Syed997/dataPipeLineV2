
FROM bitnami/spark:3.5.1

USER root
RUN apt-get update && apt-get install -y --no-install-recommends python3-pip && rm -rf /var/lib/apt/lists/*
# RUN pip3 install --no-cache-dir flask==3.0.3
RUN pip3 install --no-cache-dir \
    flask==3.0.3 \
    clickhouse-driver==0.2.9 \
    pyspark \
    pandas==2.2.2 \
    numpy==1.26.4

# Spark already includes py4j; we'll pass Kafka package at spark-submit time
USER 1001

WORKDIR /app
COPY app.py .
COPY run.sh .
# RUN chmod +x run.sh
COPY --chmod=755 run.sh .

EXPOSE 5001 4040
CMD ["/app/run.sh"]

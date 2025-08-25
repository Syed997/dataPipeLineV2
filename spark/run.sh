
#!/bin/bash
set -euo pipefail

/opt/bitnami/spark/bin/spark-submit   --master local[2]   --conf spark.ui.enabled=true   --conf spark.ui.port=4040   --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1   /app/app.py

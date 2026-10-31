# Assignment 4 — Kafka + Spark (run instructions)
1. `docker compose up -d`
2. `docker exec -it <kafka-container> /opt/kafka/bin/kafka-topics.sh --create --topic research-events --partitions 3 --replication-factor 1 --bootstrap-server localhost:9092`
3. `pip install kafka-python pyspark psutil`
4. `python producer.py` then `python consumer.py`   (Parts B, C)
5. `spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_stream.py`   (Part D)
6. `python load_test.py`   (Part E — use YOUR measured numbers)
`python common.py` prints the expected results without Kafka.

# spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark_stream.py
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import *
from common import TOPIC, BOOTSTRAP

spark = (SparkSession.builder.appName("a4-stream")
         .config("spark.sql.shuffle.partitions", "3").getOrCreate())
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("event_id", StringType()), StructField("timestamp", StringType()),
    StructField("research_group", StringType()), StructField("job_type", StringType()),
    StructField("duration_sec", DoubleType()), StructField("status", StringType()),
    StructField("cpu_pct", DoubleType())])

raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", BOOTSTRAP)
       .option("subscribe", TOPIC).option("startingOffsets", "earliest").load())

events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e"))
          .select("e.*")
          .withColumn("event_time", F.to_timestamp("timestamp")))

out = (events.withWatermark("event_time", "10 seconds")
       .groupBy(F.window("event_time", "5 seconds"), "job_type")
       .agg(F.count("*").alias("count"), F.avg("duration_sec").alias("avg_duration"))
       .orderBy("window", "job_type"))

q = (out.writeStream.outputMode("complete").format("console")
     .option("truncate", "false").start())
q.awaitTermination(60)

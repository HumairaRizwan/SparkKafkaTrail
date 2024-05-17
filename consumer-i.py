#!/usr/bin/env python

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.feature import Normalizer, StandardScaler
import random

import time
from pyspark.sql.types import *

kafka_topic_name = "ITopic"
kafka_bootstrap_servers = 'localhost:9092'

spark = SparkSession \
        .builder \
        .appName("Structured Streaming ") \
        .master("local[*]") \
        .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# Construct a streaming DataFrame that reads from topic
df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_name) \
        .load()
        
#print(flower_df.limit(10).toPandas())

flower_str_df = df.selectExpr("CAST(value AS STRING)")


#schema = StructType().add("order_id", IntegerType()).add("sepal_length", FloatType()).add("sepal_width", FloatType()).add("petal_length", #FloatType()).add("petal_width", FloatType()).add("species", StringType())

#schema2 = "order_id INT,sepal_length DOUBLE,sepal_width DOUBLE,petal_length DOUBLE,petal_width DOUBLE,species STRING"
schema3 = "order_id STRING,sepal_length STRING,sepal_width STRING,petal_length STRING,petal_width STRING,species STRING"

#flower_df = flower_str_df.select(from_csv(col("value"), schema3).alias("flower_data")).select("flower_data.*")
#flower_df.createOrReplaceTempView("flower_find");
#flower_df3 = spark.sql("SELECT * FROM flower_find")
#flower_df3.writeStream.format('delta').option('path','/home/humaira/CODE/Raccoon-AI-Engine/kafka-python/output').option('checkpointLocation', '/home/humaira/CODE/Raccoon-AI-Engine/kafka-python/check').start().awaitTermination()
#flower_df3.writeStream.format("console").outputMode("append").start().awaitTermination()


parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_csv("value", schema3).alias("data")).select("data.*") 
csv_query = parsed_df.writeStream \
   .format("csv") \
   .option("path", "/home/humaira/CODE/Raccoon-AI-Engine/kafka-python/output") \
   .option("checkpointLocation", "/home/humaira/CODE/Raccoon-AI-Engine/kafka-python/check") \
   .option("header", "true") \
   .outputMode("append") \
   .trigger(processingTime="5 minute") \
   .start()
   
csv_query.awaitTermination()


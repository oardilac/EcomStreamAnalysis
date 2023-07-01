# Databricks notebook source
# MAGIC %fs mkdirs /FileStore/train-spark/data/activity-data

# COMMAND ----------

#Leer schema
static = spark.read.json("/FileStore/train-spark/data/activity-data/")
dataSchema = static.schema #infiere schema

# COMMAND ----------

groupDf = static.groupBy("gt").count()
groupDf.show()

# COMMAND ----------

#print Schema
dataSchema

# COMMAND ----------

static.printSchema

# COMMAND ----------

streaming = spark.readStream.schema(dataSchema)\
            .option("maxFilesPerTrigger", 1)\
            .json("/FileStore/train-spark/data/activity-data")\
            .limit(10)

display(streaming)

# COMMAND ----------

activityCounts = streaming.groupBy("gt").count()

# COMMAND ----------

spark.conf.get("spark.sql.shuffle.partitions")

# COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 3)

# COMMAND ----------

#sink streaming - HOW?
activityQuery = activityCounts.writeStream.queryName("activity_counts").format("memory").outputMode("complete").start()

# COMMAND ----------

from time import sleep
for x in range(5):
    spark.sql("SELECT * FROM activity_counts").show()
    sleep(1)

# COMMAND ----------

activityQuery.awaitTermination()

# COMMAND ----------

spark.streams.active

# COMMAND ----------

# MAGIC %md
# MAGIC ##Transformations on Streams

# COMMAND ----------

# MAGIC %md
# MAGIC ###Selection and Filtering

# COMMAND ----------

# in Python
from pyspark.sql.functions import expr

simpleTransform = streaming.withColumn("stairs", expr("gt like '%stairs%'"))\
  .where("stairs is false")\
  .where("gt is not null")\
  .select("stairs","gt", "model", "arrival_time", "creation_time")\
  .writeStream\
  .queryName("simple_transform")\
  .format("memory")\
  .outputMode("append")\
  .start()

# COMMAND ----------

spark.sql("SELECT * FROM simple_transform").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Agregations

# COMMAND ----------

deviceModelStats = streaming.cube("gt", "model").avg()\
  .drop("avg(Arrival_time)")\
  .drop("avg(Creation_Time)")\
  .drop("avg(Index)")\
  .writeStream.queryName("device_counts").format("memory")\
  .outputMode("complete")\
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts

# COMMAND ----------

# MAGIC %md
# MAGIC ###Joins

# COMMAND ----------

historicalAgg = static.groupBy("gt", "model").avg()
deviceModelStats = streaming.drop("Arrival_Time", "Creation_Time", "Index")\
  .cube("gt", "model").avg()\
  .join(historicalAgg, ["gt", "model"])\
  .writeStream.queryName("device_counts1").format("memory")\
  .outputMode("complete")\
  .start()

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM device_counts1

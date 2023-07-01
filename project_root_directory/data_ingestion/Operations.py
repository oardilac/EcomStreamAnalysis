# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, current_timestamp
from pyspark.sql.functions import split
from pyspark.sql.functions import window
from pyspark.sql.types import StructType, TimestampType

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/train-spark/data/activity-data/monitoring_data

# COMMAND ----------

path = "/FileStore/train-spark/data/activity-data/"
staging_dir = 'monitoring_data'
pathOutput = f'{path}{staging_dir}'

# COMMAND ----------

userSchema = StructType()\
              .add("userA", "integer")\
              .add("userB", "integer")\
              .add("timestamp", TimestampType())\
              .add("interaction", "string")

# COMMAND ----------

# Create DataFrame representing the stream of input lines from connection to localhost:9999
activity = spark\
             .readStream\
             .option("sep", ",")\
             .schema(userSchema)\
             .csv(f'{pathOutput}/*.csv')


# COMMAND ----------

wordCounts = activity\
                .select("userB")\
                .where("interaction = \"MT\"")

# COMMAND ----------

query = wordCounts\
          .writeStream.trigger(processingTime='10 seconds')\
          .format("parquet")\
          .option("checkpointLocation", "applicationHistory") \
          .option("path",pathOutput+"/out")\
          .start()

# COMMAND ----------

query2 = wordCounts\
          .writeStream\
          .trigger(processingTime='10 seconds')\
          .format("console")\
          .start()

# COMMAND ----------

query2.awaitTermination()

# COMMAND ----------

outData = spark.read.parquet(pathOutput+"/out").limit(10)
outData.show()

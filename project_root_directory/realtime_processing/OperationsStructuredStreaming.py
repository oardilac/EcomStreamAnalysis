#import sys
#from operator import add

from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.types import StructType
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

if __name__ == "__main__":
    spark = SparkSession\
            .builder \
            .appName("Streaming")\
            .master("local[3]")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.sql.shuffle.partitions", 3)\
            .getOrCreate()
    
    spark = SparkSession.builder.appName("StreamingBuyCounter").getOrCreate()

    # TODO: Add in the schema for the data from buys.csv
    structType = StructType().add("time", "string").add("oId", "integer").add("cId", "integer")\
                .add("qty", "integer")\
                .add("price", "float")\
                .add("buy", "string")


    # TODO: read the Stream from a csv file, using the struct defined above
    filestream = spark.readStream.csv("monitoring_data", schema=structType)

    # TODO: use groupBy on the "buy" column to count the number of buys
    buySellCount = filestream.groupBy("buy").count()

    # TODO: Define the query, and output the calculations to the console in "complete" mode
    query = buySellCount.writeStream.format("console").outputMode("complete").start()
    
    query.awaitTermination()
    




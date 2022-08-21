from pyspark.sql.functions import *
from pyspark.sql.types import StructField, StructType, StringType
from pyspark.sql import SparkSession
import findspark
findspark.init()

if __name__ == "__main__":

    spark = SparkSession\
            .builder\
            .master('local[*]')\
            .appName('Kafka')\
            .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    topics = ['IssLocation']
    kofka_ip = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", topics[0]) \
        .load()

    df = kofka_ip.select("value").selectExpr("CAST(value AS STRING) as json")

    schema = StructType([
        StructField("timestamp", StringType(), False),
        StructField("message", StringType(), False),
        StructField("iss_position", StringType(), False),
    ])
    schema2 = StructType([
        StructField('latitude', StringType(), False),
        StructField('longitude', StringType(), False),
    ])

    df1 = df.withColumn("loc", from_json(df.json, schema))\
        .select("loc.*").select("iss_position").selectExpr("CAST(iss_position AS STRING) as iss_position")

    df2 = df1.withColumn("map", from_json(df1.iss_position, schema2))\
        .select("map.*")
    df2.printSchema()

    query = df2\
        .writeStream\
        .format("csv")\
        .option("header", False)\
        .option("format", "append")\
        .trigger(processingTime="50 seconds")\
        .option("checkpointLocation", "checkpoint/")\
        .option("path", "output_path/")\
        .outputMode("append")\
        .start()\
        .awaitTermination()

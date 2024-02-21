from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
from pyspark.sql import DataFrame
import logging

from pyspark.sql import Row

# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)


# Initialize Spark Session
'''
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .config("spark.es.nodes", "elasticsearch") \
    .config("spark.es.port", "9200") \
    .config("spark.es.nodes.wan.only", "true") \
    .getOrCreate()
'''


'''
spark = SparkSession \
    .builder \
    .appName("Ecommerce Dashboard") \
    .getOrCreate()

'''
#in above two ways of building sparksession object , saprk version version and scala version was incompatible with our code
#due these reasons we are  installing latest versions of scala and spark
scala_version = '2.12'
spark_version = '3.1.2'
# TODO: Ensure match above values match the correct versions
packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
]
spark = SparkSession.builder\
   .appName("Ecommerce Data Analysis")\
   .config("spark.jars.packages", ",".join(packages))\
   .getOrCreate()






spark.sparkContext.setLogLevel("ERROR")

# Kafka configuration
kafka_bootstrap_servers ='localhost:9092'

connection_uri = "mongodb://192.168.1.14:27017/ecommerce"

productViewSchema = StructType([
    StructField("view_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("cname", StringType(), True),
    StructField("pname", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", StringType(), True),
    StructField("supplier", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("view_duration", IntegerType(), True)
])
productViewDF = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                 .option("subscribe", "product_views")
                 .option("startingOffsets", "latest")
                 .load()
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json("value", productViewSchema).alias("data"))
                 .select("data.*")
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
                 .withWatermark("timestamp", "1 hour")
                 )
productViewDF = productViewDF.withColumn("processingTime", current_timestamp())
productViewDF = productViewDF.withWatermark("processingTime", "2 hours")


try:
    # Write data to MongoDB
    def write_to_mongo_pv(batchDF, batchId):
        print("insude write_to_mongo_pv")
        batchDF.write.format("mongo").mode("append").option("uri", connection_uri).option(
            "collection","product_views").save()


    writeStream = productViewDF \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_mongo_pv) \
        .option("checkpointLocation", "/tmp/spark-checkpoint1") \
        .start()

    writeStream.awaitTermination()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Stop SparkSession
    spark.stop()


from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


# In above two ways of building sparksession object , saprk version version and scala version was incompatible with our code
# due these reasons we are  installing latest versions of scala and spark
scala_version = '2.12'
spark_version = '3.1.2'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.2.1',
    'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1'
]
spark = SparkSession.builder \
    .appName("Ecommerce Data Analysis") \
    .config("spark.jars.packages", ",".join(packages)) \
    .getOrCreate()


# Kafka configuration
kafka_bootstrap_servers = 'localhost:9092'


connection_uri = "mongodb://192.168.1.13/ecommerce"


#creating dataframe schema for transactions
transactionSchema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("pname", StringType(), False),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("supplier", StringType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("country", StringType(), True),
    StructField("region", StringType(), True),
])


# Read data from 'ecommerce_transactions' topic
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")
transactionDF = transactionDF.withColumn("processingTime", current_timestamp())
transactionDF.dropna()



# transactionDF.writeStream.format("console").outputMode("append").start().awaitTermination()

# writting each batch of stream to mongodb collection

try:
    # Write data to MongoDB
    def write_to_mongo(batchDF, batchId):
        batchDF.write.format("mongo").mode("append").option("uri", connection_uri).option(
            "collection", "transactions").save()


    writeStream = transactionDF \
        .writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_mongo) \
        .option("checkpointLocation", "/tmp/spark-checkpoint") \
        .start()

    writeStream.awaitTermination()

except Exception as e:
    print(f"Error: {e}")

finally:
    # Stop SparkSession
    spark.stop()



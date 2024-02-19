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
'''
customerSchema = StructType([
    StructField("customer_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("location", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("account_created", StringType(), True),
    StructField("last_login", TimestampType(), True)
])

'''
'''
customerDF = (spark.readStream
              .format("kafka")
              .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
              .option("subscribe", "ecommerce_customers")
              .option("startingOffsets", "earliest")  # Start from the earliest records
              .load() \
              .selectExpr("CAST(value AS STRING)") \
              .select(from_json("value", customerSchema).alias("data")) \
              .select("data.*") \
              .withWatermark("last_login", "2 hours") \
              )


'''

connection_uri = "mongodb://localhost:27017/ecommerce"
collection_name = "products"
products_df = spark.read \
  .format("mongo") \
  .option("uri", connection_uri) \
  .option("collection", collection_name) \
  .load()


products_df.show(5)



'''



#writting above customerDF streaming dataframe to console
'''
'''
while True:
    query = customerDF.writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

    # Wait for query termination
    query.awaitTermination()
'''


#writting customerDF dataframe to kafka
#These is not working, do revisit this code
'''
customerDF.writeStream \
    .format("kafka") \
    .option("checkpointLocation", "file:////home/talentum/PycharmProjects/Realtime E-commerce Dashboard/kafka_writestream_checkpoint") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "customerDFconsumer") \
    .trigger(processingTime="5 seconds") \
    .start()

'''

'''


# Read data from 'ecommerce_products' topic
productSchema = StructType([
    StructField("product_id", StringType(), False),
    StructField("name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("supplier", StringType(), True),
    StructField("rating", DoubleType(), True)
])
productDF = spark.read\
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_products") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", productSchema).alias("data")) \
    .select("data.*") \
    .withColumn("processingTime", current_timestamp())  # Add processing timestamp
productDF = productDF.withWatermark("processingTime", "2 hours")

'''


# Read data from 'ecommerce_transactions' topic
transactionSchema = StructType([
    StructField("transaction_id", IntegerType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("pname",StringType(),False),
    StructField("category",StringType(),True),
    StructField("price",StringType(),True),
    StructField("supplier",StringType(),True),
    StructField("quantity", IntegerType(), True),
    StructField("date_time", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("payment_method", StringType(), True),
    StructField("country",StringType(),True),
    StructField("region",StringType(),True),
])
transactionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "earliest") \
    .option("failOnDataLoss", "false") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", transactionSchema).alias("data")) \
    .select("data.*")
transactionDF = transactionDF.withColumn("processingTime", current_timestamp())
transactionDF = transactionDF.withWatermark("processingTime", "2 hours")
#


#transactionDF.writeStream.format("console").outputMode("append").start().awaitTermination()

#writting each batch of stream to mongodb collection

try:
    # Write data to MongoDB
    def write_to_mongo(batchDF, batchId):
        batchDF.write.format("mongo").mode("append").option("uri", connection_uri).option(
            "collection","transactions").save()


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



'''
# Read data from 'ecommerce_product_views' topic
productViewSchema = StructType([
    StructField("view_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("view_duration", IntegerType(), True)
])
productViewDF = (spark.readStream
                 .format("kafka")
                 .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
                 .option("subscribe", "ecommerce_product_views")
                 .option("startingOffsets", "earliest")
                 .load()
                 .selectExpr("CAST(value AS STRING)")
                 .select(from_json("value", productViewSchema).alias("data"))
                 .select("data.*")
                 .withColumn("timestamp", col("timestamp").cast("timestamp"))
                 .withWatermark("timestamp", "1 hour")
                 )
productViewDF = productViewDF.withColumn("processingTime", current_timestamp())
productViewDF = productViewDF.withWatermark("processingTime", "2 hours")

# Read data from 'ecommerce_system_logs' topic
systemLogSchema = StructType([
    StructField("log_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("level", StringType(), True),
    StructField("message", StringType(), True)
])
systemLogDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_system_logs") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", systemLogSchema).alias("data")) \
    .select("data.*")
systemLogDF = systemLogDF.withColumn("processingTime", current_timestamp())
systemLogDF = systemLogDF.withWatermark("processingTime", "2 hours")

# Read data from 'ecommerce_user_interactions' topic
userInteractionSchema = StructType([
    StructField("interaction_id", StringType(), True),
    StructField("customer_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("interaction_type", StringType(), True),
    StructField("details", StringType(), True)
])
userInteractionDF = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
    .option("subscribe", "ecommerce_user_interactions") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(from_json("value", userInteractionSchema).alias("data")) \
    .select("data.*")
userInteractionDF = userInteractionDF.withColumn("processingTime", current_timestamp())
userInteractionDF = userInteractionDF.withWatermark("processingTime", "2 hours")

'''
'''

#writting streaming dataframe to console

query=userInteractionDF.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()
query.awaitTermination()


'''
'''

# This analysis  focus on demographics and account activity.
customerAnalysisDF = (customerDF
.groupBy(
    window(col("last_login"), "1 day"),  # Windowing based on last_login
    "gender"
)
.agg(
    count("customer_id").alias("total_customers"),
    max("last_login").alias("last_activity")
)
)

#writting streaming dataframe to console

'''
'''
query=customerAnalysisDF.writeStream \
    .outputMode('append') \
    .format('console') \
    .start()
query.awaitTermination()

'''
'''

# Analyzing product popularity and stock status with windowing
productAnalysisDF = productDF \
    .groupBy(
    window(col("processingTime"), "1 hour"),  # Window based on processingTime
    "category"
) \
    .agg(
    avg("price").alias("average_price"),
    sum("stock_quantity").alias("total_stock")
) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("category"),
    col("average_price"),
    col("total_stock")
)


#writting streaming dataframe to console

'''
'''
query=productAnalysisDF.writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()
query.awaitTermination()

'''
'''

# Analyzing sales data
salesAnalysisDF = transactionDF \
    .groupBy(
    window(col("processingTime"), "1 hour"),  # Window based on processingTime
    "product_id"
) \
    .agg(
    count("transaction_id").alias("number_of_sales"),
    sum("quantity").alias("total_quantity_sold"),
    approx_count_distinct("customer_id").alias("unique_customers")  # Use approx_count_distinct
) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("product_id"),
    col("number_of_sales"),
    col("total_quantity_sold"),
    col("unique_customers")
)



'''


#writting streaming dataframe to console

'''

query=salesAnalysisDF.writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()
query.awaitTermination()

'''


'''

# Understanding customer interest in products.
productViewsAnalysisDF = productViewDF \
    .withWatermark("timestamp", "2 hours") \
    .groupBy(
    window(col("timestamp"), "1 hour"),
    "product_id"
) \
    .agg(
    count("view_id").alias("total_views"),
    avg("view_duration").alias("average_view_duration")
) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("product_id"),
    col("total_views"),
    col("average_view_duration")
)


#writting streaming dataframe to console

'''
''' 
qquery=productViewsAnalysisDF.writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()
query.awaitTermination()

'''

'''
# User Interaction Analysis
interactionAnalysisDF = userInteractionDF \
    .withWatermark("timestamp", "2 hours") \
    .groupBy(
    window(col("timestamp"), "1 hour"),
    "interaction_type"
) \
    .agg(
    count("interaction_id").alias("total_interactions"),
    approx_count_distinct("customer_id").alias("unique_users_interacted")  # Use approx_count_distinct
) \
    .select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    col("interaction_type"),
    col("total_interactions"),
    col("unique_users_interacted")
)


#writting streaming dataframe to console
'''
'''
query=interactionAnalysisDF.writeStream \
    .outputMode('complete') \
    .format('console') \
    .start()
query.awaitTermination()

'''

#This code of writting streaming dataframe to kafka topic
# is not working see these code later
'''
interactionAnalysisDF.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "project") \
    .outputMode("append") \
    .option("checkpointLocation", "file:///home/talentum/") \
    .start() \
    .awaitTermination()
'''



'''
def writeToElasticsearch(df, index_name):
    def write_and_log(batch_df: DataFrame, batch_id: int):
        logger.info(f"Attempting to write batch {batch_id} to Elasticsearch index {index_name}.")
        try:
            if not batch_df.isEmpty():
                logger.info(f"Batch {batch_id} has data. Writing to Elasticsearch.")
                batch_df.write \
                    .format("org.elasticsearch.spark.sql") \
                    .option("checkpointLocation", f"/opt/bitnami/spark/checkpoint/{index_name}/{batch_id}") \
                    .option("es.resource", f"{index_name}/doc") \
                    .option("es.nodes", "elasticsearch") \
                    .option("es.port", "9200") \
                    .option("es.nodes.wan.only", "true") \
                    .save()
                logger.info(f"Batch {batch_id"customer_id": fake.uuid4(),
        "name": fake.name(),
        "email": fake.email(),
        "location": fake.address(),
        "age": random.randint(18, 70),
        "gender": random.choice(["Male", "Female", "Other"]),
        "account_created": fake.past_date().isoformat(),
        "last_login": fake.date_time_this_month(()).isoformat} written successfully.")
            else:
                logger.info(f"Batch {batch_id} is empty. Skipping write.")
        except Exception as e:
            logger.error(f"Error writing batch {batch_id} to Elasticsearch: {e}")
    return df.writeStream \
             .outputMode("append") \
             .foreachBatch(write_and_log) \
             .start()
writeToElasticsearch(customerAnalysisDF, "customer_analysis")
writeToElasticsearch(productAnalysisDF, "product_analysis")
writeToElasticsearch(salesAnalysisDF, "sales_analysis")
writeToElasticsearch(productViewsAnalysisDF, "product_views_analysis")
writeToElasticsearch(interactionAnalysisDF, "interaction_analysis")
spark.streams.awaitAnyTermination()# Initialize logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

'''



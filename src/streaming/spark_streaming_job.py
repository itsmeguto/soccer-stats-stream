from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, ArrayType

# Define Kafka topic and MongoDB details
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "epl-matches"
MONGO_URI = "mongodb://localhost:27017"
MONGO_DATABASE = "epl_database"
MONGO_COLLECTION = "processed_matches"

# Define schema for incoming Kafka messages
schema = StructType([
    StructField("id", StringType(), True),
    StructField("date", StringType(), True),
    StructField("name", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("competitors", ArrayType(
        StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("homeAway", StringType(), True)
        ])
    ), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("EPL Match Analysis") \
    .config("spark.mongodb.write.connection.uri", f"{MONGO_URI}/{MONGO_DATABASE}.{MONGO_COLLECTION}") \
    .getOrCreate()

# Read stream from Kafka
kafka_stream = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", KAFKA_BROKER) \
    .option("subscribe", KAFKA_TOPIC) \
    .option("startingOffsets", "earliest") \
    .load()

# Parse Kafka message value as JSON
parsed_stream = kafka_stream.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select(
        col("data.id").alias("match_id"),
        col("data.date").alias("date"),
        col("data.name").alias("short_name"),
        col("data.venue").alias("venue"),
        col("data.competitors").alias("competitors")
    )

# Extract home and away teams from competitors
processed_stream = parsed_stream.withColumn(
    "home_team",
    col("competitors")[0]["name"]
).withColumn(
    "away_team",
    col("competitors")[1]["name"]
).drop("competitors")

# Write processed data to MongoDB
mongo_writer = processed_stream.writeStream \
    .format("mongodb") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/spark-checkpoints") \
    .start()

# Wait for termination
mongo_writer.awaitTermination()

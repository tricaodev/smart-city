from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType

from config import configuration

def main():
    spark = SparkSession.builder \
        .appName("SmartCityDataStreaming") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5,"
                                       "org.apache.hadoop:hadoop-aws:3.4.1,"
                                       "com.amazonaws:aws-java-sdk:1.12.783") \
        .config("spark.hadoop.fs.s3a.access.key", configuration.get("AWS_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.secret.key", configuration.get("AWS_SECRET_KEY")) \
        .getOrCreate()

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    # Set data schema
    vehicle_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuelType", StringType(), True)
    ])

    gps_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True)
    ])

    camera_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("cameraId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("snapshot", StringType(), True)
    ])

    weather_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("location", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("weatherCondition", StringType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("windSpeed", DoubleType(), True),
        StructField("humidity", IntegerType(), True),
        StructField("airQualityIndex", DoubleType(), True)
    ])

    emergency_schema = StructType([
        StructField("id", StringType(), True),
        StructField("deviceId", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("type", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True)
    ])

    def read_kafka_topic(topic, schema):
        return spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "broker:29092") \
            .option("subscribe", topic) \
            .option("startingOffsets", "earliest") \
            .load() \
            .selectExpr("CAST(value AS STRING) AS value") \
            .select(from_json(col("value"), schema).alias("data")) \
            .select("data.*") \
            .withWatermark("timestamp", "2 minutes")

    def stream_writer(df: DataFrame, checkpoint_location, data_location):
        return df.writeStream \
            .format("parquet") \
            .option("checkpointLocation", checkpoint_location) \
            .option("path", data_location) \
            .option("outputMode", "update") \
            .start()

    # Get dataframe from kafka topic
    vehicle_df = read_kafka_topic("vehicle_data", vehicle_schema).alias("vehicle")
    gps_df = read_kafka_topic("gps_data", gps_schema).alias("gps")
    camera_df = read_kafka_topic("camera_data", camera_schema).alias("camera")
    weather_df = read_kafka_topic("weather_data", weather_schema).alias("weather")
    emergency_df = read_kafka_topic("emergency_data", emergency_schema).alias("emergency")

    # Stream data to S3 bucket
    vehicle_query = stream_writer(vehicle_df, "s3a://smart-city-streaming-data-1/checkpoint/vehicle",
                                  "s3a://smart-city-streaming-data-1/data/vehicle")
    gps_query = stream_writer(gps_df, "s3a://smart-city-streaming-data-1/checkpoint/gps",
                                            "s3a://smart-city-streaming-data-1/data/gps")
    camera_query = stream_writer(camera_df, "s3a://smart-city-streaming-data-1/checkpoint/camera",
                                 "s3a://smart-city-streaming-data-1/data/camera")
    weather_query = stream_writer(weather_df, "s3a://smart-city-streaming-data-1/checkpoint/weather",
                                 "s3a://smart-city-streaming-data-1/data/weather")
    emergency_query = stream_writer(emergency_df, "s3a://smart-city-streaming-data-1/checkpoint/emergency",
                                 "s3a://smart-city-streaming-data-1/data/emergency")

    emergency_query.awaitTermination()

if __name__ == "__main__":
    main()
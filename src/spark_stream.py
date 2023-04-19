from pyspark.sql import SparkSession

if __name__ == '__main__':

    KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"  # "kafka:9092"
    KAFKA_TOPIC = "linkedin_scrapper"

    spark = SparkSession.builder.appName("linkedin_scrapper_stream").getOrCreate()

    # Reduce logging
    spark.sparkContext.setLogLevel("WARN")

    df = spark.readStream.format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
        .writeStream \
        .format("console") \
        .outputMode("append") \
        .start() \
        .awaitTermination()
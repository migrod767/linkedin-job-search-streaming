from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


def flatten_transform(df, sep="_"):
    # compute Complex Fields (Arrays, Structs and Maptypes) in Schema
    complex_fields = dict(
        [
            (field.name, field.dataType)
            for field in df.schema.fields
            if type(field.dataType) == ArrayType
               or type(field.dataType) == StructType
               or type(field.dataType) == MapType
        ]
    )

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        # print ("Processing :"+col_name+" Type : "+str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if type(complex_fields[col_name]) == StructType:
            expanded = [
                col(col_name + "." + k).alias(col_name + sep + k)
                for k in [n.name for n in complex_fields[col_name]]
            ]
            df = df.select("*", *expanded).drop(col_name)

        # if ArrayType then add the Array Elements as Rows using the explode function
        # i.e. explode Arrays
        elif type(complex_fields[col_name]) == ArrayType:
            df = df.withColumn(col_name, explode_outer(col_name))

        # if MapType then convert all sub element to columns.
        # i.e. flatten
        elif type(complex_fields[col_name]) == MapType:
            keys_df = df.select(explode_outer(map_keys(col(col_name)))).distinct()
            keys = list(map(lambda row: row[0], keys_df.collect()))
            key_cols = list(
                map(
                    lambda f: col(col_name).getItem(f).alias(str(col_name + sep + f)),
                    keys,
                )
            )
            drop_column_list = [col_name]
            df = df.select(
                [
                    col_name
                    for col_name in df.columns
                    if col_name not in drop_column_list
                ]
                + key_cols
            )

        # recompute remaining Complex Fields in Schema
        complex_fields = dict(
            [
                (field.name, field.dataType)
                for field in df.schema.fields
                if type(field.dataType) == StructType
                   or type(field.dataType) == MapType
                # or type(field.dataType) == ArrayType

            ]
        )

    return df


if __name__ == '__main__':
    pg_user = 'mrodriguez'
    pg_pass = '5pbfHjOLTIiy99f'
    pg_url = 'postgres-database.cg7ycsilolrd.us-east-2.rds.amazonaws.com'
    pg_target_table_search = 'scrapper.job_searches_spark'
    pg_target_table_job = 'scrapper.jobs_spark'
    kafka_bootstrap_servers = "kafka:9092"
    kafka_topic_jobs = "linkedin_scrapper_job_data"
    kafka_topic_searches = "linkedin_scrapper_search_data"
    spark_postgres_jar_dir = "/opt/bitnami/spark/src/postgresql-42.3.7.jar"

    def search_batch_function(kafka_msg_df, epoch_id):
        # Json parse
        json_schemas_search = StructType([
            StructField("search_dt", StringType(), True),
            StructField("search_id", StringType(), True),
            StructField("url", StringType(), True),
            StructField("url_params",
                        StructType([StructField("f_TPR", StringType(), True),
                                    StructField("f_WT", StringType(), True),
                                    StructField("geoId", StringType(), True),
                                    StructField("keywords", StringType(), True),
                                    StructField("location", StringType(), True),
                                    StructField("pageNum", StringType(), True),
                                    StructField("position", StringType(), True),
                                    StructField("trk", StringType(), True)]),
                        True)])
        kafka_msg_df = kafka_msg_df.withColumn('vl', from_json(col('value'), json_schemas_search))

        # Flatten df
        flat_df = kafka_msg_df.select("vl")
        flat_df = flatten_transform(flat_df)

        # Rename columns
        flat_df = (
            flat_df.withColumnRenamed("vl_search_dt", "search_dt")
                .withColumnRenamed("vl_search_id", "search_id")
                .withColumnRenamed("vl_url", "search_url")
                .withColumnRenamed("vl_url_params_f_TPR", "posted_date_filter")
                .withColumnRenamed("vl_url_params_f_WT", "on_site_remote")
                .withColumnRenamed("vl_url_params_geoId", "geo_id")
                .withColumnRenamed("vl_url_params_keywords", "keywords")
                .withColumnRenamed("vl_url_params_location", "location")
        )

        # write to Postgres
        (flat_df.select("search_dt", "search_id", "search_url", "posted_date_filter",
                        "on_site_remote", "geo_id", "keywords", "location")
         .write.format("jdbc")
         .mode("append")
         .option("url", f"jdbc:postgresql://{pg_url}:5432/postgres")
         .option("driver", "org.postgresql.Driver")
         .option("dbtable", pg_target_table_search)
         .option("user", pg_user)
         .option("password", pg_pass)
         .save()
         )

        return flat_df

    # job search
    spark_js = (SparkSession
                .builder
                .master('local')
                .appName('linkedin-scrapper-streaming-search')
                # Add kafka package
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
                # Add Postgres package
                .config("spark.jars", spark_postgres_jar_dir)
                .getOrCreate()
                )

    # Reduce logging
    sc_js = spark_js.sparkContext
    sc_js.setLogLevel("WARN")

    df_js = spark_js \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_searches) \
        .option("startingOffsets", "latest") \
        .load()

    msg_js_df = df_js.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    query_js = msg_js_df.writeStream.foreachBatch(search_batch_function).start()

    # Job
    spark_j = (SparkSession
               .builder
               .master('local')
               .appName('linkedin-scrapper-streaming-job')
               # Add kafka package
               .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.5")
               # Add Postgres package
               .config("spark.jars", spark_postgres_jar_dir)
               .getOrCreate()
               )

    # Reduce logging
    sc_j = spark_j.sparkContext
    sc_j.setLogLevel("WARN")


    def job_batch_function(kafka_msg_df, epoch_id):
        # Json parse
        json_schema_job = StructType([StructField("company", StringType(), True),
                                      StructField("description", ArrayType(StringType(), True), True),
                                      StructField("description_criteria",
                                                  StructType([StructField("employment_type", StringType(), True),
                                                              StructField("industries", StringType(), True),
                                                              StructField("job_function", StringType(), True),
                                                              StructField("seniority_level", StringType(), True)]),
                                                  True),
                                      StructField("job_date_posted", StringType(), True),
                                      StructField("job_id", StringType(), True),
                                      StructField("link", StringType(), True),
                                      StructField("location", StringType(), True),
                                      StructField("page_number", LongType(), True),
                                      StructField("scrape_dt", StringType(), True),
                                      StructField("scrape_time", StringType(), True),
                                      StructField("search_id", StringType(), True),
                                      StructField("title", StringType(), True)])
        kafka_msg_df = kafka_msg_df.withColumn('vl', from_json(col('value'), json_schema_job))

        # Flatten df
        flat_df = kafka_msg_df.select("vl")
        flat_df = flatten_transform(flat_df)

        # Rename columns
        flat_df = (flat_df.withColumnRenamed("vl_search_id", "search_id")
                   .withColumnRenamed("vl_page_number", "page_number")
                   .withColumn("spark_epoch_id", lit(epoch_id))
                   .withColumnRenamed("vl_job_id", "job_id")
                   .withColumnRenamed("vl_scrape_dt", "scrape_dt")
                   .withColumnRenamed("vl_link", "job_url")
                   .withColumnRenamed("vl_job_date_posted", "job_posted_dt")
                   .withColumnRenamed("vl_title", "title")
                   .withColumnRenamed("vl_company", "company")
                   .withColumnRenamed("vl_location", "job_location")
                   .withColumnRenamed("vl_scrape_time", "scrape_time")
                   .withColumnRenamed("vl_description", "description_raw")
                   .withColumnRenamed("vl_description_criteria_seniority_level", "seniority_level")
                   .withColumnRenamed("vl_description_criteria_employment_type", "employment_type")
                   .withColumnRenamed("vl_description_criteria_job_function", "job_function")
                   .withColumnRenamed("vl_description_criteria_industries", "industries")
                   )

        # write to Postgres
        (flat_df.select("search_id", "page_number", "spark_epoch_id", "job_id", "scrape_dt", "job_url", "job_posted_dt",
                        "title", "company", "location", "scrape_time", "description_raw", "seniority_level",
                        "employment_type", "job_function", "industries")
         .write.format("jdbc")
         .mode("append")
         .option("url", f"jdbc:postgresql://{pg_url}:5432/postgres")
         .option("driver", "org.postgresql.Driver")
         .option("dbtable", pg_target_table_job)
         .option("user", pg_user)
         .option("password", pg_pass)
         .save()
         )

        return flat_df


    df_j = spark_j \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic_jobs) \
        .option("startingOffsets", "latest") \
        .load()

    msg_j_df = df_j.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    query_j = msg_j_df.writeStream.foreachBatch(job_batch_function).start()

    # Start both streams
    query_js.awaitTermination()
    # query_j.awaitTermination()

    # run multiple streams at the time https://stackoverflow.com/questions/40609771/queries-with-streaming-sources-must-be-executed-with-writestream-start

from pyspark.sql import SparkSession

pg_user = 'mrodriguez'
pg_pass = '5pbfHjOLTIiy99f'
pg_url = 'postgres-database.cg7ycsilolrd.us-east-2.rds.amazonaws.com'

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.3.7.jar") \
    .getOrCreate()

df = spark.read \
    .format("jdbc") \
    .option("url", f"jdbc:postgresql://{pg_url}:5432/postgres") \
    .option("dbtable", "test.students_test") \
    .option("user", pg_user) \
    .option("password", pg_pass) \
    .option("driver", "org.postgresql.Driver") \
    .load()


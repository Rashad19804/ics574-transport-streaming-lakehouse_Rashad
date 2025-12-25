import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "hsl_stream")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minio123")
DELTA_BUCKET = os.getenv("DELTA_BUCKET", "lakehouse")

PG_HOST = os.getenv("PG_HOST", "postgres")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB = os.getenv("PG_DB", "lakehouse")
PG_USER = os.getenv("PG_USER", "lakehouse")
PG_PASSWORD = os.getenv("PG_PASSWORD", "lakehouse")

# HSL VP payload lives under key "VP"; we forward only VP dict as JSON string.
vp_schema = StructType([
    StructField("desi", StringType()),
    StructField("dir", StringType()),
    StructField("oper", IntegerType()),
    StructField("veh", IntegerType()),
    StructField("tst", StringType()),
    StructField("tsi", LongType()),
    StructField("spd", DoubleType()),
    StructField("lat", DoubleType()),
    StructField("long", DoubleType()),
    StructField("dl", IntegerType()),
    StructField("line", IntegerType()),
    StructField("route", StringType()),
])

spark = (
    SparkSession.builder
    .appName("hsl-bronze-stream")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

# MinIO (S3A) config
hconf = spark._jsc.hadoopConfiguration()
hconf.set("fs.s3a.endpoint", MINIO_ENDPOINT)
hconf.set("fs.s3a.access.key", MINIO_ACCESS_KEY)
hconf.set("fs.s3a.secret.key", MINIO_SECRET_KEY)
hconf.set("fs.s3a.path.style.access", "true")
hconf.set("fs.s3a.connection.ssl.enabled", "false")

bronze_path = f"s3a://{DELTA_BUCKET}/bronze/hsl_vehicle_positions"
checkpoint_path = f"s3a://{DELTA_BUCKET}/_checkpoints/bronze_hsl_vp"

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_str")
    .select(from_json(col("json_str"), vp_schema).alias("vp"))
    .select("vp.*")
    .withColumn("event_time", to_timestamp(col("tst")))
    .withColumn("vehicle_id", expr("CAST(veh AS STRING)"))
    .withColumnRenamed("long", "lon")
)

# 1) Bronze Delta write
bronze_query = (
    parsed.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .start(bronze_path)
)

# 2) Latest positions to PostgreSQL (foreachBatch upsert)

def upsert_latest(batch_df, _):
    # Keep only rows with coordinates
    df = batch_df.dropna(subset=["lat", "lon", "vehicle_id"]).select(
        "vehicle_id",
        expr("CASE WHEN route IS NOT NULL THEN route ELSE CAST(line AS STRING) END").alias("route"),
        col("dir").alias("direction"),
        col("lat"),
        col("lon"),
        col("spd").alias("speed"),
        col("dl").alias("delay_seconds"),
        col("event_time"),
    )

    # Add a derived "mode" from topic isn't available here; best-effort from route prefix
    df = df.withColumn("mode", expr("NULL"))

    # Write to temp table then MERGE via JDBC; to keep it simple for class demo, we overwrite the table.
    (
        df.write
        .format("jdbc")
        .mode("overwrite")
        .option("url", f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DB}")
        .option("dbtable", "vehicle_positions_latest")
        .option("user", PG_USER)
        .option("password", PG_PASSWORD)
        .option("driver", "org.postgresql.Driver")
        .save()
    )

latest_query = (
    parsed.writeStream
    .foreachBatch(upsert_latest)
    .outputMode("update")
    .start()
)

bronze_query.awaitTermination()
latest_query.awaitTermination()

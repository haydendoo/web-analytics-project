# Glue ETL Job that streams data from kinesis to transform and send over to s3
import sys
from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructType, StructField, DoubleType, LongType

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Kinesis source
# --------------------------------------------------

kinesis_df = (
    spark.readStream
    .format("kinesis")
    .option("streamName", "YOUR_KINESIS_STREAM_NAME")
    .option("region", "ap-southeast-1")
    .option("startingPosition", "LATEST")
    .load()
)

# --------------------------------------------------
# Parse JSON from Kinesis
# --------------------------------------------------

json_df = kinesis_df.selectExpr(
    "CAST(data AS STRING) as json_str"
)

parsed_df = json_df.select(
    F.from_json(
        "json_str",
        "session_id STRING, event_type STRING, element STRING, element_id STRING, x DOUBLE, y DOUBLE, timestamp STRING"
    ).alias("data")
).select("data.*")

# --------------------------------------------------
# Convert timestamp
# --------------------------------------------------

parsed_df = parsed_df.withColumn(
    "timestamp",
    F.to_timestamp("timestamp")
)

# --------------------------------------------------
# Checkout coordinates
# --------------------------------------------------

CHECKOUT_X = 237
CHECKOUT_Y = 127

parsed_df = parsed_df.withColumn(
    "distance_to_checkout",
    F.sqrt(
        F.pow(F.col("x") - CHECKOUT_X, 2) +
        F.pow(F.col("y") - CHECKOUT_Y, 2)
    )
)

# --------------------------------------------------
# SESSION WINDOWING (IMPORTANT CHANGE)
# --------------------------------------------------

from pyspark.sql.window import Window

sessionized = parsed_df.withWatermark("timestamp", "10 minutes")

features = (
    sessionized.groupBy(
        "session_id",
        F.window("timestamp", "10 minutes")
    )
    .agg(

        F.count("*").alias("total_events"),

        F.sum(
            F.when(F.col("element") == "BUTTON", 1).otherwise(0)
        ).alias("total_button_clicks"),

        F.max(
            F.when(F.col("event_type") == "conversion", 1).otherwise(0)
        ).alias("converted"),

        (
            F.unix_timestamp(F.max("timestamp")) -
            F.unix_timestamp(F.min("timestamp"))
        ).alias("session_duration_sec"),

        F.avg("distance_to_checkout")
         .alias("avg_distance_to_checkout"),

        F.countDistinct("element_id")
         .alias("unique_elements_clicked"),

        F.avg(
            F.when(F.col("element") == "BUTTON", 1).otherwise(0)
        ).alias("button_click_ratio")

    )
)

# --------------------------------------------------
# Write to S3 (STREAMING)
# --------------------------------------------------

query = (
    features.writeStream
    .format("parquet")
    .outputMode("append")
    .option(
        "path",
        "s3://analytics-transformed-parquet-12891/output/"
    )
    .option(
        "checkpointLocation",
        "s3://analytics-transformed-parquet-12891/checkpoints/glue-stream/"
    )
    .trigger(processingTime="60 seconds")
    .start()
)

query.awaitTermination()

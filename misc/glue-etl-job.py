# Glue ETL Job Code to transform data into more useful data
import sys

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import functions as F

# --------------------------------------------------
# Glue boilerplate
# --------------------------------------------------

args = getResolvedOptions(sys.argv, ["JOB_NAME"])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# --------------------------------------------------
# Read from Glue Catalog
# --------------------------------------------------

raw_dyf = glueContext.create_dynamic_frame.from_catalog(
    database="ree",
    table_name="wvdsvsvsvsd"
)

df = raw_dyf.toDF()

# --------------------------------------------------
# Convert timestamp
# --------------------------------------------------

df = df.withColumn(
    "timestamp",
    F.to_timestamp("timestamp")
)

# --------------------------------------------------
# Checkout button coordinates
# --------------------------------------------------

CHECKOUT_X = 237
CHECKOUT_Y = 127

# --------------------------------------------------
# Distance from checkout button
# --------------------------------------------------

df = df.withColumn(
    "distance_to_checkout",
    F.sqrt(
        F.pow(F.col("x") - CHECKOUT_X, 2)
        + F.pow(F.col("y") - CHECKOUT_Y, 2)
    )
)

# --------------------------------------------------
# Session-level feature engineering
# --------------------------------------------------

features = (
    df.groupBy("session_id")
      .agg(

          # Total events
          F.count("*").alias("total_events"),

          # Total button clicks
          F.sum(
              F.when(F.col("element") == "BUTTON", 1)
               .otherwise(0)
          ).alias("total_button_clicks"),

          # Converted? (0 or 1)
          F.max(
              F.when(F.col("event_type") == "conversion", 1)
               .otherwise(0)
          ).alias("converted"),

          # Session duration
          (
              F.unix_timestamp(F.max("timestamp"))
              - F.unix_timestamp(F.min("timestamp"))
          ).alias("session_duration_sec"),

          # Average distance to checkout button
          F.avg("distance_to_checkout")
           .alias("avg_distance_to_checkout"),

          # Optional extra features

          F.countDistinct("element_id")
           .alias("unique_elements_clicked"),

          F.avg(
              F.when(F.col("element") == "BUTTON", 1)
               .otherwise(0)
          ).alias("button_click_ratio")

      )
)

# --------------------------------------------------
# Write Parquet
# --------------------------------------------------

(
    features.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(
            "s3://analytics-transformed-parquet-12891/output/"
        )
)

job.commit()

import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, avg, max, min, rank, round, count
from pyspark.sql.window import Window

SILVER_PATH = "s3://SILVER-BUCKET/data/"
GOLD_BEST_MARKETS = "s3://GOLD-BUCKET/best_markets/"
GOLD_STATE_SUMMARY = "s3://GOLD-BUCKET/state_summary/"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df = spark.read.parquet(SILVER_PATH)

# Top 5 highest-paying markets per commodity per day
w_rank = Window.partitionBy("commodity", "record_date").orderBy(col("modal_price").desc())

best_markets = df \
    .withColumn("price_rank", rank().over(w_rank)) \
    .filter(col("price_rank") <= 5) \
    .select(
        "commodity", "record_date", "state", "district", "market",
        "modal_price", "min_price", "max_price", "price_rank",
        "price_trend", "price_change_pct"
    )

# State-level price statistics
state_summary = df \
    .groupBy("state", "commodity", "record_date") \
    .agg(
        round(avg("modal_price"), 2).alias("avg_price"),
        max("modal_price").alias("max_price"),
        min("modal_price").alias("min_price"),
        count("*").alias("market_count")
    ) \
    .withColumn("price_spread", round(col("max_price") - col("min_price"), 2))

best_markets.write.mode("overwrite").partitionBy("commodity").parquet(GOLD_BEST_MARKETS)
state_summary.write.mode("overwrite").partitionBy("state", "commodity").parquet(GOLD_STATE_SUMMARY)

job.commit()

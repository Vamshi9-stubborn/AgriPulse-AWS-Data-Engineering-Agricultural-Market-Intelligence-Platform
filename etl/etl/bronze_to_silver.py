import sys
import re
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, lag, when, to_date, round
from pyspark.sql.window import Window
from pyspark.sql.types import DoubleType

BRONZE_PATH = "s3://BRONZE-BUCKET/raw/"
SILVER_PATH = "s3://SILVER-BUCKET/data/"

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

spark.sparkContext._jsc.hadoopConfiguration().set(
    "mapreduce.input.fileinputformat.input.dir.recursive", "true"
)

raw_rdd = spark.sparkContext.textFile(BRONZE_PATH)
fixed_rdd = raw_rdd.map(lambda line: re.sub(r'\}\{', '}\n{', line)) \
                   .flatMap(lambda line: line.split('\n'))
df = spark.read.json(fixed_rdd)

cleaned = df \
    .withColumn("min_price", col("min_price").cast(DoubleType())) \
    .withColumn("max_price", col("max_price").cast(DoubleType())) \
    .withColumn("modal_price", col("modal_price").cast(DoubleType())) \
    .withColumn("record_date", to_date(col("record_date"), "dd/MM/yyyy")) \
    .filter(col("commodity").isNotNull()) \
    .dropDuplicates(["market", "commodity", "record_date"])

w = Window.partitionBy("market", "commodity").orderBy("record_date")

enriched = cleaned \
    .withColumn("prev_price", lag("modal_price").over(w)) \
    .withColumn(
        "price_change_pct",
        when(col("prev_price").isNotNull() & col("modal_price").isNotNull(),
             round((col("modal_price") - col("prev_price")) / col("prev_price") * 100, 2))
        .otherwise(None)) \
    .withColumn(
        "price_trend",
        when(col("price_change_pct") > 5, "Rising")
        .when(col("price_change_pct") < -5, "Falling")
        .when(col("price_change_pct").isNotNull(), "Stable")
        .otherwise("No price data"))

enriched.write \
    .mode("overwrite") \
    .partitionBy("state", "commodity") \
    .parquet(SILVER_PATH)

job.commit()

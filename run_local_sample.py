from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("SparkLogAnalysisSample") \
    .master("local[*]") \
    .getOrCreate()

# Read all log files under sample
logs_df = spark.read.text("data/sample/application_1485248649253_0052/*.log")

# Example: filter INFO lines
info_lines = logs_df.filter(logs_df.value.contains("INFO"))
info_lines.show(10, truncate=False)

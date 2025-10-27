"""
Problem 1 (CLUSTER): Log level distribution over the full dataset in S3.

Reads from the bucket:
  s3a://<SPARK_LOGS_BUCKET without scheme>/data/application_*/*.log

Writes (driver writes via pandas):
  data/output/problem1_counts.csv
  data/output/problem1_sample.csv
  data/output/problem1_summary.txt

Also copies the three files to ~/spark-cluster/ for easy scp.
"""

import os
import sys
import time
import shutil
import logging
from pathlib import Path
from typing import Tuple

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, input_file_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] {%(filename)s:%(lineno)d} - %(message)s",
)
log = logging.getLogger("problem1_cluster")

def build_spark(master_url: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("Problem1_Cluster_LogLevels")
        .master(master_url)
        # cluster sizing
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        # S3A
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider",
        )
        # perf
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )

def resolve_paths() -> Tuple[str, str]:
    """Return (input_path, output_dir)."""
    bucket = os.environ.get("SPARK_LOGS_BUCKET")
    if not bucket:
        raise ValueError(
            "SPARK_LOGS_BUCKET not set. Example:\n"
            "  export SPARK_LOGS_BUCKET=s3://ea973-assignment-spark-cluster-logs"
        )
    bucket_clean = bucket.replace("s3://", "").replace("s3a://", "")
    input_path = f"s3a://{bucket_clean}/data/application_*/*.log"
    output_dir = "data/output"
    os.makedirs(output_dir, exist_ok=True)
    return input_path, output_dir

def main():
    print("=" * 70)
    print("PROBLEM 1 (CLUSTER): Log Level Distribution")
    print("=" * 70)

    # master URL
    if len(sys.argv) > 1:
        master_url = sys.argv[1]
    else:
        ip = os.getenv("MASTER_PRIVATE_IP")
        if not ip:
            print("❌ Master URL required. Usage:")
            print("   uv run python problem1_cluster.py spark://<MASTER_PRIVATE_IP>:7077")
            return 1
        master_url = f"spark://{ip}:7077"

    print(f"Connecting to Spark Master at: {master_url}")
    input_path, output_dir = resolve_paths()
    log.info(f"Input: {input_path}")

    start = time.time()
    success = False
    spark = None

    try:
        spark = build_spark(master_url)
        spark.sparkContext.setLogLevel("WARN")

        logs_df = spark.read.text(input_path).withColumn("file_path", input_file_name())
        total_lines = logs_df.count()
        log.info(f"Loaded {total_lines:,} total log lines")

        parsed_df = (
            logs_df
            .select(
                regexp_extract(col("value"), r"(INFO|WARN|ERROR|DEBUG)", 1).alias("log_level"),
                col("value").alias("log_entry"),
            )
            .filter(col("log_level") != "")
            .cache()
        )

        with_level = parsed_df.count()
        log.info(f"Parsed {with_level:,} log entries with levels")

        counts_df = parsed_df.groupBy("log_level").count().orderBy(col("count").desc())

        # ---- Driver-side writes (tiny outputs) ----
        counts_pdf = counts_df.toPandas()
        sample_pdf = parsed_df.orderBy(rand()).limit(10).toPandas()

        counts_csv  = Path(output_dir) / "problem1_counts_cluster.csv"
        sample_csv  = Path(output_dir) / "problem1_sample_cluster.csv"
        summary_txt = Path(output_dir) / "problem1_summary_cluster.txt"

        counts_pdf.to_csv(counts_csv, index=False)
        sample_pdf.to_csv(sample_csv, index=False)

        counts_dict = {r["log_level"]: int(r["count"]) for _, r in counts_pdf.iterrows()}
        with summary_txt.open("w") as f:
            f.write("Environment: CLUSTER\n")
            f.write(f"Total log lines processed: {total_lines:,}\n")
            f.write(f"Lines with log levels: {with_level:,}\n\n")
            for lvl in ["INFO", "WARN", "ERROR", "DEBUG"]:
                if lvl in counts_dict:
                    cnt = counts_dict[lvl]
                    pct = (cnt / with_level) * 100 if with_level else 0
                    f.write(f"{lvl:<6}: {cnt:>12,} ({pct:6.2f}%)\n")

        # Copy to ~/spark-cluster for scp per assignment
        cluster_out = Path.home() / "spark-cluster"
        cluster_out.mkdir(exist_ok=True)
        shutil.copyfile(counts_csv,  cluster_out / "problem1_counts_cluster.csv")
        shutil.copyfile(sample_csv,  cluster_out / "problem1_sample_cluster.csv")
        shutil.copyfile(summary_txt, cluster_out / "problem1_summary_cluster.txt")

        success = True
    except Exception as e:
        log.exception("❌ Error during cluster run")
        print(f"❌ {e}")
    finally:
        if spark:
            spark.stop()

    print("\n" + "=" * 70)
    if success:
        print("✅ PROBLEM 1 COMPLETED SUCCESSFULLY!")
        print("Files created in data/output/ and copied to ~/spark-cluster/:")
        print("  • problem1_counts_cluster.csv")
        print("  • problem1_sample_cluster.csv")
        print("  • problem1_summary_cluster.txt")
    else:
        print("❌ FAILURE — check logs above")
    print("=" * 70)
    print(f"Elapsed: {time.time() - start:.2f}s")
    return 0 if success else 1

if __name__ == "__main__":
    raise SystemExit(main())

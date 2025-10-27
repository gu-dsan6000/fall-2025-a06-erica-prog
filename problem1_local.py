"""
Problem 1 (LOCAL): Log level distribution using the small sample in data/sample/.

Reads:
  data/sample/application_*/*.log

Writes (driver writes via pandas):
  data/output/problem1_counts.csv
  data/output/problem1_sample.csv
  data/output/problem1_summary.txt
"""

import os
import time
import logging
from pathlib import Path

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_extract, col, rand, input_file_name

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] {%(filename)s:%(lineno)d} - %(message)s",
)
log = logging.getLogger("problem1_local")

def build_spark_local() -> SparkSession:
    # Plain local session
    return (
        SparkSession.builder
        .appName("Problem1_Local_LogLevels")
        .getOrCreate()
    )

def main():
    print("=" * 70)
    print("PROBLEM 1 (LOCAL): Log Level Distribution")
    print("=" * 70)

    # Paths
    input_path = "data/sample/application_*/*.log"
    output_dir = Path("data/output")
    output_dir.mkdir(parents=True, exist_ok=True)

    spark = build_spark_local()
    spark.sparkContext.setLogLevel("WARN")

    start = time.time()
    log.info(f"Reading: {input_path}")
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

    # ---- Write outputs via driver (pandas) ----
    counts_pdf = counts_df.toPandas()
    sample_pdf = parsed_df.orderBy(rand()).limit(10).toPandas()

    (output_dir / "problem1_counts_local.csv").write_text(
        counts_pdf.to_csv(index=False)
    )  # type: ignore[attr-defined]  # for mypy silence

    (output_dir / "problem1_sample_local.csv").write_text(
        sample_pdf.to_csv(index=False)
    )  # type: ignore[attr-defined]

    # summary
    counts_dict = {r["log_level"]: int(r["count"]) for _, r in counts_pdf.iterrows()}
    summary_path = output_dir / "problem1_summary_local.txt"
    with summary_path.open("w") as f:
        f.write(f"Environment: LOCAL\n")
        f.write(f"Total log lines processed: {total_lines:,}\n")
        f.write(f"Lines with log levels: {with_level:,}\n\n")
        for lvl in ["INFO", "WARN", "ERROR", "DEBUG"]:
            if lvl in counts_dict:
                cnt = counts_dict[lvl]
                pct = (cnt / with_level) * 100 if with_level else 0
                f.write(f"{lvl:<6}: {cnt:>12,} ({pct:6.2f}%)\n")

    spark.stop()
    print("\n✅ LOCAL run complete. Files in data/output/:")
    print("  • problem1_counts_local.csv")
    print("  • problem1_sample_local.csv")
    print("  • problem1_summary_local.txt")
    print(f"\nElapsed: {time.time() - start:.2f}s")
    return 0

# tiny helpers for Path.write_text with pandas string
from typing import Any
def _write_text(self: Path, s: str) -> None:
    self.parent.mkdir(parents=True, exist_ok=True)
    with self.open("w") as f:
        f.write(s)
setattr(Path, "write_text", _write_text)  # monkey-patch for brevity

if __name__ == "__main__":
    raise SystemExit(main())

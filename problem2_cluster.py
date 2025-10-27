#!/usr/bin/env python3
# problem2_cluster.py
#
# Full-cluster version of Problem 2: Cluster Usage Analysis
#
# Reads (from your bucket):
#   s3a://<SPARK_LOGS_BUCKET without scheme>/data/application_*/*.log
#
# Writes (driver writes via pandas) to data/output/:
#   - problem2_timeline.csv
#   - problem2_cluster_summary.csv
#   - problem2_stats.txt
#   - problem2_bar_chart.png
#   - problem2_density_plot.png
#
# Also copies the five files to ~/spark-cluster/ for easy scp.
#
# Usage:
#   uv run python problem2_cluster.py spark://$MASTER_PRIVATE_IP:7077 --net-id YOUR_NET_ID
#   uv run python problem2_cluster.py --skip-spark   # regenerate plots from existing CSV

import os
import sys
import time
import shutil
import argparse
import logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, col, to_timestamp,
    min as smin, max as smax, count
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] {%(filename)s:%(lineno)d} - %(message)s",
)
log = logging.getLogger("problem2_cluster")


# ---------------- Spark ----------------
def build_spark(master_url: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName("Problem2_ClusterUsage_CLUSTER")
        .master(master_url)
        # cluster sizing
        .config("spark.executor.memory", "4g")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.cores", "2")
        .config("spark.cores.max", "6")
        # S3A (uses instance profile)
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.InstanceProfileCredentialsProvider"
        )
        # performance
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .getOrCreate()
    )


# ----------- timeline extraction -------
def extract_timeline(spark: SparkSession, s3_glob: str):
    log.info(f"Reading logs from S3: {s3_glob}")
    df = spark.read.text(s3_glob).withColumn("file_path", input_file_name())

    # robust: extract IDs from FILE PATH
    df = df.select(
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("ts_str"),
        regexp_extract(col("file_path"), r"application_(\d+)_\d+", 1).alias("cluster_id"),
        regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1).alias("application_id"),
    ).filter(col("ts_str") != "")

    df = df.withColumn("timestamp", to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss")) \
           .drop("ts_str") \
           .cache()

    # start/end per application
    timeline = (df.groupBy("cluster_id", "application_id")
                  .agg(smin("timestamp").alias("start_time"),
                       smax("timestamp").alias("end_time")))

    # duration & app_number (recreate from application_id AFTER grouping)
    timeline = (timeline
                .withColumn("duration_min",
                            (col("end_time").cast("long") - col("start_time").cast("long")) / 60.0)
                .withColumn("app_number",
                            regexp_extract(col("application_id"), r"application_\d+_(\d+)", 1))
               )
    log.info(f"✅ Timeline rows: {timeline.count():,}")
    return timeline


# ----------------- viz -----------------
def make_visualizations(timeline_csv: str, outdir: Path):
    sns.set(style="whitegrid")
    df = pd.read_csv(timeline_csv)

    # ensure cluster_id is str for plotting
    df["cluster_id"] = df["cluster_id"].astype(str)

    # bar: apps per cluster (value labels)
    counts = (df.groupby("cluster_id")["application_id"].count()
                .reset_index(name="num_applications")
                .sort_values("num_applications", ascending=False))

    plt.figure(figsize=(12, 6))
    ax = sns.barplot(data=counts, x="cluster_id", y="num_applications",
                     hue="cluster_id", dodge=False, legend=False)
    ax.set_title("Applications per Cluster")
    ax.set_xlabel("Cluster ID"); ax.set_ylabel("Number of Applications")
    for p in ax.patches:
        h = p.get_height()
        ax.annotate(f"{int(h)}", (p.get_x() + p.get_width()/2, h),
                    ha="center", va="bottom", fontsize=9)
    plt.tight_layout()
    bar_path = outdir / "problem2_bar_chart_cluster.png"
    plt.savefig(bar_path, dpi=150); plt.close()

    # density: durations for largest cluster (log x)
    largest = counts.iloc[0]["cluster_id"]
    dfl = df[df["cluster_id"] == largest].copy()
    dfl = dfl[dfl["duration_min"] > 0]

    plt.figure(figsize=(12, 6))
    sns.histplot(dfl["duration_min"], bins=30, kde=True)
    plt.xscale("log")
    plt.title(f"Duration Distribution (Cluster {largest})  (n={len(dfl)})")
    plt.xlabel("Duration (minutes, log scale)"); plt.ylabel("Count")
    plt.tight_layout()
    dens_path = outdir / "problem2_density_plot_cluster.png"
    plt.savefig(dens_path, dpi=150); plt.close()

    log.info(f"📊 saved: {bar_path}")
    log.info(f"📊 saved: {dens_path}")
    return str(bar_path), str(dens_path)


# ---------------- helpers ----------------
def resolve_s3_input_glob() -> str:
    bucket = os.environ.get("SPARK_LOGS_BUCKET", "")
    if not bucket:
        raise ValueError(
            "SPARK_LOGS_BUCKET not set. Example:\n"
            "  export SPARK_LOGS_BUCKET=s3://ea973-assignment-spark-cluster-logs"
        )
    bucket = bucket.replace("s3://", "").replace("s3a://", "")
    return f"s3a://{bucket}/data/application_*/*.log"


def copy_to_cluster_drop(paths):
    drop = Path.home() / "spark-cluster"
    drop.mkdir(exist_ok=True)
    for p in paths:
        shutil.copy2(p, drop / Path(p).name)


# ---------------- main -----------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("master_url", nargs="?", default=None)
    parser.add_argument("--net-id", required=False)        # accepted but unused
    parser.add_argument("--skip-spark", action="store_true",
                        help="Skip Spark; regenerate plots from existing CSV")
    args = parser.parse_args()

    outdir = Path("data/output")
    outdir.mkdir(parents=True, exist_ok=True)

    # fast path: only rebuild plots
    if args.skip_spark:
        timeline_csv = outdir / "problem2_timeline_cluster.csv"
        if not timeline_csv.exists():
            print("❌ --skip-spark needs data/output/problem2_timeline_cluster.csv present.")
            return 1
        make_visualizations(str(timeline_csv), outdir)
        copy_to_cluster_drop([
            outdir / "problem2_bar_chart_cluster.png",
            outdir / "problem2_density_plot_cluster.png",
        ])
        print("✅ Regenerated plots from existing CSV.")
        return 0

    # resolve master URL
    if not args.master_url:
        ip = os.getenv("MASTER_PRIVATE_IP")
        if not ip:
            print("❌ Provide Spark master URL: spark://<IP>:7077")
            return 1
        args.master_url = f"spark://{ip}:7077"

    s3_glob = resolve_s3_input_glob()
    log.info(f"Using Spark master: {args.master_url}")
    log.info(f"Input path: {s3_glob}")

    spark = None
    t0 = time.time()
    try:
        spark = build_spark(args.master_url)
        spark.sparkContext.setLogLevel("WARN")

        # 1) Build timeline
        timeline = extract_timeline(spark, s3_glob).select(
            "cluster_id", "application_id", "app_number",
            "start_time", "end_time", "duration_min"
        )

        # 2) Save timeline CSV
        timeline_csv = outdir / "problem2_timeline_cluster.csv"
        timeline.toPandas().to_csv(timeline_csv, index=False)
        log.info(f"📁 {timeline_csv}")

        # 3) Cluster summary CSV
        cluster_summary = (timeline.groupBy("cluster_id")
                           .agg(count("application_id").alias("num_applications"),
                                smin("start_time").alias("cluster_first_app"),
                                smax("end_time").alias("cluster_last_app")))
        cluster_csv = outdir / "problem2_cluster_summary_cluster.csv"
        cluster_summary.toPandas().to_csv(cluster_csv, index=False)
        log.info(f"📁 {cluster_csv}")

        # 4) Stats file
        n_clusters = cluster_summary.count()
        n_apps = timeline.count()
        avg_apps = n_apps / n_clusters if n_clusters else 0
        ordered = cluster_summary.orderBy(col("num_applications").desc()).toPandas()

        stats_txt = outdir / "problem2_stats_cluster.txt"
        with stats_txt.open("w") as f:
            f.write(f"Total unique clusters: {n_clusters}\n")
            f.write(f"Total applications: {n_apps}\n")
            f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
            f.write("Most heavily used clusters:\n")
            for _, r in ordered.iterrows():
                f.write(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications\n")
        log.info(f"📄 {stats_txt}")

        # 5) Visualizations
        bar_png, dens_png = make_visualizations(str(timeline_csv), outdir)

        # 6) Copy to ~/spark-cluster for scp
        copy_to_cluster_drop([timeline_csv, cluster_csv, stats_txt, bar_png, dens_png])

        print("\n" + "=" * 70)
        print("✅ PROBLEM 2 COMPLETED SUCCESSFULLY!")
        print("Files created in data/output/ and copied to ~/spark-cluster/:")
        print("  • problem2_timeline_cluster.csv")
        print("  • problem2_cluster_summary_cluster.csv")
        print("  • problem2_stats_cluster.txt")
        print("  • problem2_bar_chart_cluster.png")
        print("  • problem2_density_plot_cluster.png")
        print("=" * 70)
        print(f"Elapsed: {time.time() - t0:.2f}s")
        return 0

    except Exception as e:
        log.exception("❌ Error during cluster run")
        print(f"❌ {e}")
        return 1
    finally:
        if spark:
            spark.stop()


if __name__ == "__main__":
    sys.exit(main())

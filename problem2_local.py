# Input  : data/sample/application_*/*.log
# Outputs: data/output/
#   - problem2_timeline_local.csv
#   - problem2_cluster_summary_local.csv
#   - problem2_stats_local.txt
#   - problem2_bar_chart_local.png
#   - problem2_density_plot_local.png

import os, time, logging
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    input_file_name, regexp_extract, col, to_timestamp,
    min as smin, max as smax, count
)

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] {%(filename)s:%(lineno)d} - %(message)s")
log = logging.getLogger("problem2_local")

# ---------------- Spark ----------------
def build_spark() -> SparkSession:
    return (SparkSession.builder
            .appName("Problem2_ClusterUsage_LOCAL")
            .config("spark.sql.adaptive.enabled", "true")
            .getOrCreate())

# ----------- timeline extraction -------
def extract_timeline(spark: SparkSession, glob_path: str):
    log.info(f"Reading logs from: {glob_path}")
    df = spark.read.text(glob_path).withColumn("file_path", input_file_name())

    # pull identifiers from the FILE PATH (more reliable than line text)
    df = df.select(
        regexp_extract(col("value"), r"^(\d{2}/\d{2}/\d{2} \d{2}:\d{2}:\d{2})", 1).alias("ts_str"),
        regexp_extract(col("file_path"), r"application_(\d+)_\d+", 1).alias("cluster_id"),
        regexp_extract(col("file_path"), r"(application_\d+_\d+)", 1).alias("application_id"),
    ).filter(col("ts_str") != "")

    df = df.withColumn("timestamp", to_timestamp(col("ts_str"), "yy/MM/dd HH:mm:ss")).drop("ts_str").cache()

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
def make_visualizations(timeline_csv: str, outdir: str, suffix: str):
    sns.set(style="whitegrid")
    df = pd.read_csv(timeline_csv)

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
    bar_path = os.path.join(outdir, f"problem2_bar_chart_{suffix}.png")
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
    dens_path = os.path.join(outdir, f"problem2_density_plot_{suffix}.png")
    plt.savefig(dens_path, dpi=150); plt.close()

    log.info(f"📊 saved: {bar_path}")
    log.info(f"📊 saved: {dens_path}")

# ---------------- main -----------------
def main():
    outdir = "data/output"; os.makedirs(outdir, exist_ok=True)
    glob_path = "data/sample/application_*/*.log"

    spark = build_spark(); spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    timeline = extract_timeline(spark, glob_path).select(
        "cluster_id","application_id","app_number","start_time","end_time","duration_min"
    )

    # CSVs
    timeline_csv = os.path.join(outdir, "problem2_timeline_local.csv")
    timeline.toPandas().to_csv(timeline_csv, index=False)
    log.info(f"📁 {timeline_csv}")

    cluster_summary = (timeline.groupBy("cluster_id")
                       .agg(count("application_id").alias("num_applications"),
                            smin("start_time").alias("cluster_first_app"),
                            smax("end_time").alias("cluster_last_app")))
    cluster_csv = os.path.join(outdir, "problem2_cluster_summary_local.csv")
    cluster_summary.toPandas().to_csv(cluster_csv, index=False)
    log.info(f"📁 {cluster_csv}")

    # stats
    n_clusters = cluster_summary.count()
    n_apps = timeline.count()
    avg_apps = n_apps / n_clusters if n_clusters else 0
    ordered = cluster_summary.orderBy(col("num_applications").desc()).toPandas()

    stats_txt = os.path.join(outdir, "problem2_stats_local.txt")
    with open(stats_txt, "w") as f:
        f.write(f"Total unique clusters: {n_clusters}\n")
        f.write(f"Total applications: {n_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps:.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for _, r in ordered.iterrows():
            f.write(f"  Cluster {r['cluster_id']}: {int(r['num_applications'])} applications\n")
    log.info(f"📄 {stats_txt}")

    # plots
    make_visualizations(timeline_csv, outdir, "local")

    spark.stop()
    log.info(f"✅ Done in {time.time()-t0:.1f}s")
    return 0

if __name__ == "__main__":
    import sys; sys.exit(main())

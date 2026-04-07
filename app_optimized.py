import argparse
import json
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


def build_session(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.memory", "1g")
        .config("spark.executor.memory", "1g")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--metrics", default="metrics_optimized.json")
    args = parser.parse_args()

    spark = build_session("Lab2-Optimized")
    start = time.time()

    print("[INFO] Reading CSV from HDFS")
    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(args.input)
    )

    print("[INFO] Optimized cleanup")
    df = (
        df.dropDuplicates()
          .withColumn("Quantity", F.col("Quantity").cast("double"))
          .withColumn("UnitPrice", F.col("UnitPrice").cast("double"))
          .withColumn("CustomerID", F.col("CustomerID").cast("string"))
          .withColumn("amount", (F.col("Quantity") * F.col("UnitPrice")).cast("double"))
          .filter(F.col("Country").isNotNull())
          .filter(F.col("InvoiceNo").isNotNull())
          .filter(F.col("StockCode").isNotNull())
          .filter(F.col("Quantity").isNotNull())
          .filter(F.col("UnitPrice").isNotNull())
          .filter(F.col("amount").isNotNull())
          .repartition(8, "Country")
          .persist(StorageLevel.MEMORY_AND_DISK)
    )

    rows_after_cleanup = df.count()

    print("[INFO] Aggregation 1: by Country")
    agg1 = df.groupBy("Country").agg(
        F.count("*").alias("rows_cnt"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.round(F.sum("amount"), 2).alias("sum_amount"),
        F.round(F.avg("Quantity"), 2).alias("avg_quantity"),
    )

    print("[INFO] Aggregation 2: by StockCode")
    agg2 = df.groupBy("StockCode").agg(
        F.count("*").alias("rows_cnt"),
        F.round(F.sum("amount"), 2).alias("sum_amount")
    )

    print("[INFO] Aggregation 3: by CustomerID")
    agg3 = (
        df.filter(F.col("CustomerID").isNotNull())
          .groupBy("CustomerID")
          .agg(
              F.count("*").alias("orders_cnt"),
              F.round(F.sum("amount"), 2).alias("total_spent")
          )
    )

    print("[INFO] Statistics")
    stats = df.agg(
        F.round(F.avg("amount"), 2).alias("avg_amount_global"),
        F.round(F.max("amount"), 2).alias("max_amount"),
        F.round(F.min("amount"), 2).alias("min_amount")
    )

    print("[INFO] Writing results")
    agg1.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.output}/by_country")
    agg2.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.output}/by_stockcode")
    agg3.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.output}/by_customer")
    stats.coalesce(1).write.mode("overwrite").option("header", True).csv(f"{args.output}/stats")

    elapsed = round(time.time() - start, 3)

    metrics = {
        "app": "optimized",
        "input": args.input,
        "output": args.output,
        "execution_time_sec": elapsed,
        "rows_after_cleanup": rows_after_cleanup,
        "country_groups": agg1.count(),
        "stockcode_groups": agg2.count(),
        "customer_groups": agg3.count(),
        "partitions": df.rdd.getNumPartitions(),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }

    with open(args.metrics, "w", encoding="utf-8") as f:
        json.dump(metrics, f, ensure_ascii=False, indent=2)

    print("[INFO] Done")
    print(json.dumps(metrics, ensure_ascii=False, indent=2))

    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()

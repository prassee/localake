import argparse
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_SOURCE_PATH = "s3://stage/heimdall/heimdall/upi_transactions"
DEFAULT_TABLE = "nessie.heimdall_1_sync_nessie_public.upi_transactions"
DEFAULT_CHECKPOINT = "s3a://warehouse/checkpoints/stream_upi_transactions"


def build_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config(
            "spark.sql.catalog.nessie.catalog-impl",
            "org.apache.iceberg.nessie.NessieCatalog",
        )
        .config("spark.sql.catalog.nessie.uri", "http://nessie:19120/api/v1")
        .config("spark.sql.catalog.nessie.ref", "main")
        .config("spark.sql.catalog.nessie.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3n.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )


def run_stream(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    checkpoint_path: str,
    trigger_seconds: int,
    max_files_per_trigger: int,
    merge_keys: List[str],
    latest_timestamp_col: str,
) -> None:

    if not spark.catalog.tableExists(table_name):
        raise ValueError(f"Target table '{table_name}' does not exist.")

    target_schema = spark.read.table(table_name).schema
    source_columns = [f.name for f in target_schema.fields]

    resolved_ts_col = (
        latest_timestamp_col
        if latest_timestamp_col in source_columns
        else "_olake_timestamp"
    )

    merge_on = " AND ".join([f"t.`{c}` <=> s.`{c}`" for c in merge_keys])

    immutable_columns = set(merge_keys)
    immutable_columns.add("date_created")

    update_columns = [c for c in source_columns if c not in immutable_columns]

    update_clause = ",\n".join([f"t.`{c}` = s.`{c}`" for c in update_columns])

    insert_columns = ", ".join([f"`{c}`" for c in source_columns])
    insert_values = ", ".join([f"s.`{c}`" for c in source_columns])

    stream_df = (
        spark.readStream.format("parquet")
        .schema(target_schema)
        .option("recursiveFileLookup", "true")
        .option("maxFilesPerTrigger", max_files_per_trigger)
        .load(source_path)
    )

    def upsert_micro_batch(batch_df, batch_id):

        if batch_df.isEmpty():
            return

        batch_spark = batch_df.sparkSession

        latest_window = Window.partitionBy(*merge_keys).orderBy(
            F.col(resolved_ts_col).desc_nulls_last(),
            F.col("_cdc_timestamp").desc_nulls_last(),
            F.col("_cdc_lsn").desc_nulls_last(),
        )

        dedup_df = (
            batch_df.withColumn("_rn", F.row_number().over(latest_window))
            .filter("_rn = 1")
            .drop("_rn")
            .withColumn("date_created_day", F.to_date("date_created"))
            .persist()
        )

        affected_days = [
            r.date_created_day
            for r in dedup_df.select("date_created_day").distinct().collect()
        ]

        print(f"Batch {batch_id}: {len(affected_days)} affected partitions")

        for day in affected_days:
            day_view = f"batch_{batch_id}_{day:%Y%m%d}"

            (
                dedup_df.filter(F.col("date_created_day") == F.lit(day))
                .drop("date_created_day")
                .createOrReplaceGlobalTempView(day_view)
            )

            merge_sql = f"""
MERGE INTO {table_name} t
USING global_temp.{day_view} s
ON {merge_on}
WHEN MATCHED THEN
UPDATE SET
{update_clause}
WHEN NOT MATCHED THEN
INSERT ({insert_columns})
VALUES ({insert_values})
"""

            batch_spark.sql(merge_sql)

            batch_spark.catalog.dropGlobalTempView(day_view)

        dedup_df.unpersist()

    (
        stream_df.writeStream.foreachBatch(upsert_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
        .awaitTermination()
    )


def run_stream_v0(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    checkpoint_path: str,
    trigger_seconds: int,
    max_files_per_trigger: int,
    merge_keys: List[str],
    latest_timestamp_col: str,
) -> None:
    if not spark.catalog.tableExists(table_name):
        raise ValueError(
            f"Target table '{table_name}' does not exist. Create it first (e.g. via backfill_upi_to_iceberg.py)."
        )

    source_schema = spark.read.table(table_name).schema
    source_columns = [field.name for field in source_schema.fields]

    missing_keys = [key for key in merge_keys if key not in source_columns]
    if missing_keys:
        raise ValueError(
            f"Merge key column(s) not found in target schema: {missing_keys}"
        )

    resolved_ts_col = latest_timestamp_col
    if resolved_ts_col not in source_columns:
        if "event_ts" in source_columns:
            resolved_ts_col = "event_ts"
        else:
            raise ValueError(
                f"Timestamp column '{latest_timestamp_col}' not found in target schema and no 'event_ts' fallback available"
            )

    on_keys = list(merge_keys)
    # if "date_created" not in on_keys:
    #     on_keys.append("day(date_created)")

    missing_on_keys = [key for key in on_keys if key not in source_columns]
    if missing_on_keys:
        raise ValueError(
            f"ON clause column(s) not found in target schema: {missing_on_keys}"
        )

    on_clause = " AND ".join([f"t.`{key}` <=> s.`{key}`" for key in on_keys])
    update_columns = [col for col in source_columns if col not in merge_keys]
    update_clause = ",\n            ".join(
        [f"t.`{col}` = s.`{col}`" for col in update_columns]
    )
    insert_columns = ", ".join([f"`{col}`" for col in source_columns])
    insert_values = ", ".join([f"s.`{col}`" for col in source_columns])

    stream_df = (
        spark.readStream.format("parquet")
        .schema(source_schema)
        .option("recursiveFileLookup", "true")
        .option("maxFilesPerTrigger", str(max_files_per_trigger))
        .load(source_path)
    )

    def upsert_micro_batch(batch_df, batch_id: int) -> None:
        if batch_df.limit(1).count() == 0:
            return

        batch_spark = batch_df.sparkSession

        latest_per_key_window = Window.partitionBy(
            *[F.col(key) for key in merge_keys]
        ).orderBy(
            F.col(resolved_ts_col).desc_nulls_last(),
            F.col("_cdc_timestamp").desc_nulls_last(),
            F.col("_cdc_lsn").desc_nulls_last(),
        )
        dedup_df = (
            batch_df.withColumn("_rn", F.row_number().over(latest_per_key_window))
            .filter(F.col("_rn") == 1)
            .drop("_rn")
        )
        source_view = f"stream_source_batch_{batch_id}"
        dedup_df.createOrReplaceTempView(source_view)
        dc_on_clause = (
            on_clause + """ AND day(t.date_created) = day(s.date_created) """
        )  # Use the outer scope on_clause
        merge_sql = f"""
        MERGE INTO {table_name} t
        USING {source_view} s
        ON {dc_on_clause}
        """

        if update_columns:
            merge_sql += f"""
        WHEN MATCHED THEN UPDATE SET
            {update_clause}
            """

        merge_sql += f"""
        WHEN NOT MATCHED THEN INSERT ({insert_columns})
        VALUES ({insert_values})
        """

        batch_spark.sql(merge_sql)
        batch_spark.catalog.dropTempView(source_view)

    query = (
        stream_df.writeStream.foreachBatch(upsert_micro_batch)
        .option("checkpointLocation", checkpoint_path)
        .trigger(processingTime=f"{trigger_seconds} seconds")
        .start()
    )

    print(
        f"Streaming started from {source_path} -> {table_name} with merge keys: {merge_keys} and latest timestamp column: {resolved_ts_col}"
    )
    print(f"Checkpoint: {checkpoint_path}")
    query.awaitTermination()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Stream parquet files from S3/MinIO path and MERGE into an existing Iceberg table."
    )
    parser.add_argument("--source-path", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--checkpoint", default=DEFAULT_CHECKPOINT)
    parser.add_argument("--trigger-seconds", type=int, default=15)
    parser.add_argument("--max-files-per-trigger", type=int, default=50)
    parser.add_argument("--merge-keys", default="id")
    parser.add_argument("--latest-timestamp-col", default="_olake_timestamp")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("s3-parquet-to-iceberg-stream")
    run_stream(
        spark=spark,
        source_path=args.source_path,
        table_name=args.table,
        checkpoint_path=args.checkpoint,
        trigger_seconds=args.trigger_seconds,
        max_files_per_trigger=args.max_files_per_trigger,
        merge_keys=[key.strip() for key in args.merge_keys.split(",") if key.strip()],
        latest_timestamp_col=args.latest_timestamp_col,
    )


if __name__ == "__main__":
    main()

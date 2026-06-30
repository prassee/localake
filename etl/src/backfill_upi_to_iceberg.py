import argparse
from typing import List

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEFAULT_SOURCE_PATH = "s3://stage/heimdall/heimdall/upi_transactions/*/*/*.parquet"
DEFAULT_TABLE = "nessie.heimdall_1_sync_nessie_public.upi_transactions_v2"
DEFAULT_MERGE_KEYS = "id"
DEFAULT_LATEST_TS_COL = "_olake_timestamp"


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


def ensure_namespace(spark: SparkSession, table_name: str) -> None:
    namespace = ".".join(table_name.split(".")[:2])
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def recreate_target_table(spark: SparkSession, source_df, table_name: str) -> None:
    source_columns = set(source_df.columns)
    if "date_created" not in source_columns:
        raise ValueError(
            "Source data must contain 'date_created' to create the table partitioned by day(date_created)"
        )
    if "id" not in source_columns:
        raise ValueError(
            "Source data must contain 'id' to apply WRITE ORDERED BY id ASC NULLS FIRST"
        )

    source_view = "backfill_source_schema"
    source_df.limit(0).createOrReplaceTempView(source_view)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(
        f"""
        CREATE TABLE {table_name}
        USING iceberg
        PARTITIONED BY (days(date_created))
        AS SELECT * FROM {source_view}
        """
    )
    spark.sql(f"ALTER TABLE {table_name} WRITE ORDERED BY id ASC NULLS FIRST")
    spark.catalog.dropTempView(source_view)


def run_backfill(
    spark: SparkSession,
    source_path: str,
    table_name: str,
    merge_keys: List[str],
    latest_timestamp_col: str,
) -> None:
    ensure_namespace(spark, table_name)

    source_df = (
        spark.read.option("recursiveFileLookup", "true")
        .format("parquet")
        .load(source_path)
    )

    if source_df.limit(1).count() == 0:
        print(f"No source rows found at {source_path}; nothing to backfill.")
        return

    recreate_target_table(spark, source_df, table_name)

    target_schema = spark.read.table(table_name).schema
    target_columns = [field.name for field in target_schema.fields]

    missing_keys = [key for key in merge_keys if key not in target_columns]
    if missing_keys:
        raise ValueError(
            f"Merge key column(s) not found in target schema: {missing_keys}"
        )

    resolved_ts_col = latest_timestamp_col
    if resolved_ts_col not in target_columns:
        if "event_ts" in target_columns:
            resolved_ts_col = "event_ts"
        else:
            raise ValueError(
                f"Timestamp column '{latest_timestamp_col}' not found in target schema and no 'event_ts' fallback available"
            )

    source_df = source_df.select(*target_columns)

    latest_per_key_window = Window.partitionBy(
        *[F.col(key) for key in merge_keys]
    ).orderBy(F.col(resolved_ts_col).desc_nulls_last())
    dedup_df = (
        source_df.withColumn("_rn", F.row_number().over(latest_per_key_window))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    source_view = "backfill_source_latest"
    dedup_df.createOrReplaceTempView(source_view)

    on_clause = " AND ".join([f"t.`{key}` <=> s.`{key}`" for key in merge_keys])
    update_columns = [col for col in target_columns if col not in merge_keys]
    update_clause = ",\n            ".join(
        [f"t.`{col}` = s.`{col}`" for col in update_columns]
    )
    insert_columns = ", ".join([f"`{col}`" for col in target_columns])
    insert_values = ", ".join([f"s.`{col}`" for col in target_columns])

    merge_sql = f"""
    MERGE INTO {table_name} t
    USING {source_view} s
    ON {on_clause}
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

    print(
        f"Running one-time backfill from {source_path} into {table_name} using keys {merge_keys} and latest timestamp column {resolved_ts_col}"
    )
    spark.sql(merge_sql)
    spark.catalog.dropTempView(source_view)
    print("Backfill completed.")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="One-time parquet backfill into Iceberg table using MERGE upsert semantics."
    )
    parser.add_argument("--source-path", default=DEFAULT_SOURCE_PATH)
    parser.add_argument("--table", default=DEFAULT_TABLE)
    parser.add_argument("--merge-keys", default=DEFAULT_MERGE_KEYS)
    parser.add_argument("--latest-timestamp-col", default=DEFAULT_LATEST_TS_COL)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    spark = build_spark("s3-parquet-to-iceberg-backfill")
    run_backfill(
        spark=spark,
        source_path=args.source_path,
        table_name=args.table,
        merge_keys=[key.strip() for key in args.merge_keys.split(",") if key.strip()],
        latest_timestamp_col=args.latest_timestamp_col,
    )
    spark.stop()


if __name__ == "__main__":
    main()

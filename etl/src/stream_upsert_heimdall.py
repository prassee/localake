"""
Structured Streaming MOR upsert into nessie.db.heimdall.

Reads the CSV files that generate_faker_csv.py drops under s3a://stage/<index>/
as a streaming file source, and upserts each micro-batch into the merge-on-read
Iceberg table via MERGE INTO (run inside foreachBatch).

Why foreachBatch:
    Spark's native streaming Iceberg sink only supports append output. To perform
    a merge-on-read upsert (which writes position-delete files on updates) we run a
    MERGE INTO statement against the table for every micro-batch DataFrame.

Note on delete-file type:
    Spark MERGE in MOR mode emits POSITION deletes (files metadata content = 1),
    NOT equality deletes (content = 2). Equality deletes require Flink upsert or the
    Iceberg Java EqualityDeleteWriter.
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType
from pyspark.sql.window import Window

STAGE_GLOB = "s3a://stage/*/"  # per-index subdirs: stage/0/, stage/1/, ...
PATH_GLOB_FILTER = "faker_data_*.csv"
CHECKPOINT = "s3a://warehouse/checkpoints/heimdall_stream/"
TARGET_TABLE = "nessie.db.heimdall"

# Streaming CSV requires an explicit schema (no inference). Read everything as
# string and cast inside the batch, mirroring the batch loader.
CSV_SCHEMA = StructType(
    [
        StructField("id", StringType()),
        StructField("event_ts", StringType()),
        StructField("payload", StringType()),
    ]
)


def build_spark():
    return (
        SparkSession.builder.appName("heimdall-stream-upsert")
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
        .getOrCreate()
    )


def create_heimdall_table(spark):
    """Create the merge-on-read heimdall Iceberg table if it does not exist."""
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS nessie.db")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {TARGET_TABLE} (
            id        BIGINT,
            event_ts  TIMESTAMP,
            payload   STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        TBLPROPERTIES (
            'format-version'    = '2',
            'write.delete.mode' = 'merge-on-read',
            'write.update.mode' = 'merge-on-read',
            'write.merge.mode'  = 'merge-on-read'
        )
        """
    )


def upsert_micro_batch(batch_df, batch_id):
    """foreachBatch handler: MOR upsert this micro-batch into the target table."""
    spark = batch_df.sparkSession

    df = batch_df.withColumn("id", F.col("id").cast("long")).withColumn(
        "event_ts", F.col("event_ts").cast("timestamp")
    )

    # Collapse duplicate ids within the batch to the latest event_ts so MERGE has a
    # single source row per id (Iceberg rejects multiple source matches per target).
    latest_per_id = Window.partitionBy("id").orderBy(F.col("event_ts").desc())
    df_latest = (
        df.withColumn("_rn", F.row_number().over(latest_per_id))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_latest.createOrReplaceTempView("heimdall_stream_src")
    spark.sql(
        f"""
        MERGE INTO {TARGET_TABLE} AS t
        USING heimdall_stream_src AS s
        ON t.id = s.id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print(f"[batch {batch_id}] MOR upsert committed to {TARGET_TABLE}.")


def main():
    spark = build_spark()
    create_heimdall_table(spark)

    stream = (
        spark.readStream.schema(CSV_SCHEMA)
        .option("header", "true")
        .option("pathGlobFilter", PATH_GLOB_FILTER)
        # Cap files per micro-batch -> more, smaller MOR commits (small-files lab).
        .option("maxFilesPerTrigger", 20)
        .csv(STAGE_GLOB)
    )

    query = (
        stream.writeStream.foreachBatch(upsert_micro_batch)
        .option("checkpointLocation", CHECKPOINT)
        # availableNow: drain all currently-staged files in micro-batches, then stop.
        # Swap for .trigger(processingTime="30 seconds") to run continuously.
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()

    spark.sql(
        f"SELECT count(*) AS rows, count(DISTINCT id) AS distinct_ids FROM {TARGET_TABLE}"
    ).show()
    spark.stop()


if __name__ == "__main__":
    main()

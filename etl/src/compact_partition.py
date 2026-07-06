from datetime import datetime, timedelta

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.appName("iceberg-partition-compaction")
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
    .config(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    .getOrCreate()
)

table_name = "db.heimdall"
table_name = "heimdall_1_sync_nessie_public.upi_transactions"
table_name = "localake.upi_transactions"
table_name = "localake_unnest_1m_nessie_public.upi_transactions"


def compact_partition(spark, start_ts, end_ts):
    # Run compaction on the specified event_ts window.
    def _as_datetime(value):
        if isinstance(value, datetime):
            return value
        if isinstance(value, str):
            return datetime.fromisoformat(value)
        raise TypeError("start_ts/end_ts must be datetime or ISO timestamp string")

    start_dt = _as_datetime(start_ts)
    end_dt = _as_datetime(end_ts)
    start_ts_str = start_dt.strftime("%Y-%m-%d %H:%M:%S")
    end_ts_plus_one = (end_dt + timedelta(days=1)).strftime("%Y-%m-%d %H:%M:%S")

    print(f"Running compaction for range: {start_ts_str} -> {end_ts_plus_one}")
    target_file_size_bytes = 512 * 1024 * 1024  # 512 MB
    sql = f"""
    CALL nessie.system.rewrite_data_files(        
        table => '{table_name}',
        where => "date_modified >= TIMESTAMP '{start_ts_str}' AND date_modified < TIMESTAMP '{end_ts_plus_one}'",
        strategy => 'binpack',
        options => map('target-file-size-bytes', '{target_file_size_bytes}' , 
        'partial-progress.enabled', 'false', 
        'delete-file-threshold',    '1')
    )
    """
    spark.sql(sql)
    print(f"Compaction completed for range: {start_ts_str} -> {end_ts_plus_one}")


def enable_gc_on_table(spark):
    """
    Enable garbage collection (GC) on the Iceberg table to allow snapshot expiration and orphan file removal.
    """
    print(f"Enabling GC on table {table_name}...")
    spark.sql(
        f""" ALTER TABLE nessie.{table_name} SET TBLPROPERTIES ('gc.enabled'='true')"""
    )
    print("GC enabled on table.")


def expire_old_snapshots(spark, mins=1):
    """
    Expire Iceberg snapshots older than the specified number of minutes (default: 1).
    """
    from datetime import datetime, timedelta

    expire_ts = datetime.utcnow() - timedelta(minutes=mins)
    expire_str = expire_ts.strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Expiring snapshots older than {expire_str} (UTC)")
    sql = f"""
    CALL nessie.system.expire_snapshots(
        table => '{table_name}',
        older_than => TIMESTAMP '{expire_str}'
    )
    """
    spark.sql(sql)
    print("Expired old snapshots.")


def remove_orphan_files(spark, older_than_mins=1):
    """
    Remove orphan files using the Action API, which allows intervals shorter than 24 hours.
    Safe to use when there are no concurrent writes on the table.
    """
    from datetime import datetime, timedelta

    expire_ts = datetime.utcnow() - timedelta(minutes=older_than_mins)
    older_than_ms = int(expire_ts.timestamp() * 1000)
    expire_str = expire_ts.strftime("%Y-%m-%dT%H:%M:%S")
    print(f"Removing orphan files older than {expire_str} (UTC)")
    jvm = spark._jvm
    iceberg_table = jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(
        spark._jsparkSession, f"nessie.{table_name}"
    )
    result = (
        jvm.org.apache.iceberg.spark.actions.SparkActions.get(spark._jsparkSession)
        .deleteOrphanFiles(iceberg_table)
        .olderThan(older_than_ms)
        .execute()
    )
    print(
        f"Removed orphan files: {result.orphanFileLocations().size()} file(s) deleted."
    )


def pre_check_files(spark, partition_date):
    df = spark.sql(
        f"SELECT COUNT(*), MIN(event_ts), MAX(event_ts) FROM nessie.{table_name} WHERE event_ts >= TIMESTAMP '{partition_date} 00:00:00' AND event_ts < TIMESTAMP '{partition_date} 23:59:59'"
    )
    df.show(truncate=False)


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 3:
        print("Usage: python compact_partition.py <start_date> <end_date>")
        sys.exit(1)
    partition_s_date = sys.argv[1]
    partition_e_date = sys.argv[2]
    # iterator over date range and compact each partition
    start_dt = datetime.strptime(partition_s_date, "%Y-%m-%d")
    end_dt = datetime.strptime(partition_e_date, "%Y-%m-%d")
    # compact_partition(spark, start_dt, end_dt)
    # delta = end_dt - start_dt
    # for i in range(delta.days + 1):
    #     partition_date = (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
    #     print(f"Processing compaction for partition date: {partition_date}")
    #     # compact_partition(spark, partition_date)
    #     # pre_check_files(spark, partition_date)

    # Enable GC before expiring snapshots or removing orphan files
    enable_gc_on_table(spark)
    # expire_old_snapshots(spark, mins=2)  # Expire snapshots older than 2 minutes
    remove_orphan_files(spark)

    spark.stop()

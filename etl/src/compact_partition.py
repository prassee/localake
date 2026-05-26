from datetime import timedelta
from datetime import datetime
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
    .getOrCreate()
)

table_name = "db.heimdall"

def compact_partition(spark, partition_date):
    # Run compaction on the specified partition
    print(f"Running compaction on partition: {partition_date}")
    # Use event_ts column for partition predicate (no event_ts_day column)
    start_dt = datetime.strptime(partition_date, "%Y-%m-%d")
    end_dt = start_dt + timedelta(days=1)
    start_ts = start_dt.strftime("%Y-%m-%d 00:00:00")
    end_ts = end_dt.strftime("%Y-%m-%d 00:00:00")
    target_file_size_bytes = 5 * 1024 * 1024  # 5 MB
    sql = f"""
    CALL nessie.system.rewrite_data_files(        
        table => '{table_name}',
        where => "event_ts >= TIMESTAMP '{start_ts}' AND event_ts < TIMESTAMP '{end_ts}'",
        strategy => 'binpack',
        options => map('target-file-size-bytes', '{target_file_size_bytes}')
    )
    """
    spark.sql(sql)
    print(f"Compaction completed for partition: {partition_date}")    

def enable_gc_on_table(spark):
    """
    Enable garbage collection (GC) on the Iceberg table to allow snapshot expiration and orphan file removal.
    """
    print(f"Enabling GC on table {table_name}...")
    spark.sql(f"""
        ALTER TABLE nessie.{table_name} SET TBLPROPERTIES ('gc.enabled'='true')
    """)
    print("GC enabled on table.")

def expire_old_snapshots(spark, hours=1):
    """
    Expire Iceberg snapshots older than the specified number of hours (default: 24).
    """
    from datetime import datetime, timedelta
    expire_ts = datetime.utcnow() - timedelta(minutes=hours)
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
        jvm.org.apache.iceberg.spark.actions.SparkActions
        .get(spark._jsparkSession)
        .deleteOrphanFiles(iceberg_table)
        .olderThan(older_than_ms)
        .execute()
    )
    print(f"Removed orphan files: {result.orphanFileLocations().size()} file(s) deleted.")

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
    delta = end_dt - start_dt
    for i in range(delta.days + 1):
        partition_date = (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
        print(f"Processing compaction for partition date: {partition_date}")
        # compact_partition(spark, partition_date)
        # pre_check_files(spark, partition_date)

    # Enable GC before expiring snapshots or removing orphan files
    # enable_gc_on_table(spark)
    # expire_old_snapshots(spark)
    remove_orphan_files(spark)

    spark.stop()

from pyspark.sql import SparkSession

STAGE_BUCKET   = "s3a://stage/upi_events/"
ICEBERG_TABLE  = "nessie.db.upi_events"
NAMESPACE      = "nessie.db"


def build_spark():
    return (
        SparkSession.builder
        .appName("load-upi-events")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
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


def ensure_table(spark):
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {NAMESPACE}")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {ICEBERG_TABLE} (
            id          BIGINT,
            event_ts    TIMESTAMP,
            user_id     STRING,
            upi_id      STRING,
            event_type  STRING,
            bank        STRING,
            amount      DOUBLE,
            status      STRING,
            device_id   STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        TBLPROPERTIES (
            'write.parquet.compression-codec' = 'snappy',
            'gc.enabled' = 'true'
        )
    """)


def load(spark):
    print(f"Reading parquet files from {STAGE_BUCKET} ...")
    df = spark.read.option("recursiveFileLookup", "true").parquet(STAGE_BUCKET)
    print(f"Schema: {df.schema.simpleString()}")
    print(f"Row count: {df.count():,}")

    print(f"Appending to {ICEBERG_TABLE} ...")
    df.writeTo(ICEBERG_TABLE).append()
    print("Load complete.")


if __name__ == "__main__":
    spark = build_spark()
    ensure_table(spark)
    load(spark)
    spark.sql(f"SELECT event_type, COUNT(*) AS cnt FROM {ICEBERG_TABLE} GROUP BY event_type ORDER BY cnt DESC").show(truncate=False)
    spark.stop()

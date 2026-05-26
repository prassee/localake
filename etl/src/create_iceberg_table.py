from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

def ensure_namespace(spark, namespace="nessie.db"):
    """
    Create the Iceberg/Nessie namespace if it does not exist.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")

    
def main():
    spark = (
        SparkSession.builder
        .appName("create-iceberg-with-nessie")
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

    # Ensure the namespace exists
    ensure_namespace(spark, "nessie.db")

    # Create the table if it doesn't exist
    spark.sql(
        """
        CREATE TABLE IF NOT EXISTS nessie.db.small_file_test (
            id BIGINT,
            event_ts TIMESTAMP,
            payload STRING
        )
        USING iceberg
        PARTITIONED BY (days(event_ts))
        """
    )

    # Append a few sample rows
    df = (
        spark.range(5)
        .withColumn("event_ts", current_timestamp())
        .withColumn("payload", lit("sample_payload"))
    )

    df.writeTo("nessie.db.small_file_test").append()

    print("Wrote sample rows; preview:")
    spark.sql("SELECT * FROM nessie.db.small_file_test LIMIT 10").show(truncate=False)

    spark.stop()


if __name__ == '__main__':
    main()

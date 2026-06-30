from pyspark.sql import SparkSession


def ensure_namespace(spark, namespace="nessie.db"):
    """
    Create the Iceberg/Nessie namespace if it does not exist.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def list_namespaces(spark, catalog="nessie"):
    """
    List all namespaces available in the configured Nessie catalog.
    """
    namespaces_df = spark.sql(f"SHOW NAMESPACES IN {catalog}")
    return [row[0] for row in namespaces_df.collect()]


def list_tables_in_namespace(
    spark, catalog="nessie", namespace="heimdall_1_sync_nessie_public"
):
    """
    List all tables in a specific namespace within the configured Nessie catalog.
    """
    tables_df = spark.sql(f"SHOW TABLES IN {catalog}.{namespace}")
    return [row[1] for row in tables_df.collect()]


def main():
    spark = (
        SparkSession.builder.appName("create-iceberg-with-nessie")
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

    # Ensure the namespace exists
    # ensure_namespace(spark, "nessie.db")

    namespaces = list_namespaces(spark, "nessie")
    print("Namespaces in Nessie:", namespaces)

    tables = list_tables_in_namespace(spark, "nessie", "heimdall_1_sync_nessie_public")
    print("Tables in namespace 'heimdall_1_sync_nessie_public':", tables)

    spark.stop()


if __name__ == "__main__":
    main()

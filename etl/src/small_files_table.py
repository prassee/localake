from pyspark.sql import SparkSession

# from pyspark.sql.functions import lit


def print_all_tables_in_namespace(spark, namespace="nessie.db"):
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    print(f"Tables in namespace '{namespace}':")
    for row in tables:
        print(f"- {row.tableName}")


def ensure_namespace(spark, namespace="nessie.db"):
    """
    Create the Iceberg/Nessie namespace if it does not exist.
    """
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {namespace}")


def drop_all_tables_in_namespace(spark, namespace="nessie.db"):
    tables = spark.sql(f"SHOW TABLES IN {namespace}").collect()
    for row in tables:
        table_name = row.tableName
        print(f"Dropping table and data: {namespace}.{table_name}")
        # Use PURGE to delete data files from S3 as well
        spark.sql(f"DROP TABLE IF EXISTS {namespace}.{table_name}")


def list_matching_files(spark, s3_path):
    """Return the list of file paths matching the given Hadoop glob pattern."""
    hadoop_conf = spark._jsc.hadoopConfiguration()
    jvm = spark._jvm
    path = jvm.org.apache.hadoop.fs.Path(s3_path)
    fs = path.getFileSystem(hadoop_conf)
    statuses = fs.globStatus(path)
    if statuses is None:
        return []
    return [status.getPath().toString() for status in statuses]


def create_heimdall_table(spark):
    """Create the merge-on-read heimdall Iceberg table if it does not exist."""
    spark.sql(
        """
		CREATE TABLE IF NOT EXISTS nessie.db.heimdall (
			id BIGINT,
			event_ts TIMESTAMP,
			payload STRING
		)
		USING iceberg
		PARTITIONED BY (days(event_ts))
		TBLPROPERTIES (
			'format-version' = '2',
			'write.delete.mode' = 'merge-on-read',
			'write.update.mode' = 'merge-on-read',
			'write.merge.mode'  = 'merge-on-read'
		)
		"""
    )


def populate_table_from_csv(spark, s3_path, run_ddl=False):
    if run_ddl:
        create_heimdall_table(spark)

    # Read all rows from the CSV file on S3 using Spark
    df_csv = spark.read.option("header", True).csv(s3_path)
    import pyspark.sql.functions as F
    from pyspark.sql.window import Window

    # Cast columns to correct types
    df_csv = df_csv.withColumn("id", df_csv.id.cast("long"))
    df_csv = df_csv.withColumn("event_ts", df_csv.event_ts.cast("timestamp"))

    # Collapse duplicate ids in the source to the row with the latest event_ts,
    # so MERGE has a single source row per id (Iceberg rejects multiple matches).
    latest_per_id = Window.partitionBy("id").orderBy(F.col("event_ts").desc())
    df_latest = (
        df_csv.withColumn("_rn", F.row_number().over(latest_per_id))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_latest.createOrReplaceTempView("heimdall_source")

    # Upsert by id, keeping the entry with the latest event_ts. An existing row is
    # only overwritten when the incoming event_ts is newer.
    spark.sql(
        """
        MERGE INTO nessie.db.heimdall AS t
        USING heimdall_source AS s
        ON t.id = s.id
        WHEN MATCHED AND s.event_ts > t.event_ts THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    print("Merged source into nessie.db.heimdall (upsert by id, latest event_ts wins).")


def main():
    spark = (
        SparkSession.builder.appName("iceberg-small-files-lab")
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
    # print_all_tables_in_namespace(spark, "nessie.db")
    # drop_all_tables_in_namespace(spark, "nessie.db")
    #
    #
    #
    # (spark, "nessie.db")

    # Create the table if it doesn't exist
    START_DATE = "2025-01-01"
    END_DATE = "2026-06-26"

    from datetime import datetime, timedelta

    start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
    end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
    delta_days = (end_dt - start_dt).days + 1

    ddl_done = False
    for day in range(delta_days):
        date_dt = start_dt + timedelta(days=day)
        date_str = date_dt.strftime("%Y-%m-%d")
        # Glob across all index dirs (stage/<index>/) for this date's CSVs,
        # then process each matched file individually.
        s3_glob = f"s3a://stage/*/faker_data_{date_str}*.csv"
        files = list_matching_files(spark, s3_glob)
        if not files:
            print(f"No CSV files matched {s3_glob}; skipping {date_str}.")
            continue

        print(f"Processing date: {date_str} ({len(files)} file(s))")
        for s3_file in files:
            # Run the table DDL only on the very first file processed.
            populate_table_from_csv(spark, s3_file, run_ddl=not ddl_done)
            ddl_done = True

    spark.stop()


if __name__ == "__main__":
    main()

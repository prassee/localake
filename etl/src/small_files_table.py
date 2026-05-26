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

def populate_table_from_csv(spark,s3_path):
	spark.sql(
		"""
		CREATE TABLE IF NOT EXISTS nessie.db.heimdall (
			id BIGINT,
			event_ts TIMESTAMP,
			payload STRING		
		)
		USING iceberg
		PARTITIONED BY (days(event_ts))
		"""
	)


	# Read all rows from the CSV file on S3 using Spark
	df_csv = spark.read.option("header", True).csv(s3_path)
	import pyspark.sql.functions as F

	# Cast columns to correct types
	df_csv = df_csv.withColumn("id", df_csv.id.cast("long"))
	df_csv = df_csv.withColumn("event_ts", df_csv.event_ts.cast("timestamp"))
	part_count = 1
	# Add a synthetic bucket column to force shuffle
	df_csv = df_csv.withColumn("bucket", (F.rand() * part_count).cast("int"))

	(
		df_csv.repartition("bucket")
		.drop("bucket")
		.writeTo("nessie.db.heimdall")
		.option("write.distribution-mode", "none")
		.append()
	)
	print(f"Wrote {part_count} small files to a single partition.")

	# Validate number of data files in Iceberg table
	file_count = spark.sql("SELECT count(*) FROM nessie.db.heimdall.files").collect()[0][0]
	print(f"Actual data files in Iceberg table: {file_count}")

def main():
	spark = (
		SparkSession.builder
		.appName("iceberg-small-files-lab")
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
	# ensure_namespace(spark, "nessie.db")
	# print_all_tables_in_namespace(spark, "nessie.db")
	# drop_all_tables_in_namespace(spark, "nessie.db")
	# print_all_tables_in_namespace(spark, "nessie.db")

	# Create the table if it doesn't exist
	START_DATE = "2026-05-05"
	END_DATE = "2026-05-14"

	from datetime import datetime, timedelta
	start_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
	end_dt = datetime.strptime(END_DATE, "%Y-%m-%d")
	delta_days = (end_dt - start_dt).days + 1

	for i in range(5):
		for day in range(delta_days):
			date_dt = start_dt + timedelta(days=day)
			date_str = date_dt.strftime('%Y-%m-%d')
			print(f"Processing date: {date_str}")
			s3_path = f"s3a://stage/{i}/faker_data_{date_str}*.csv"
			# Example: call the function once (original behavior)
			populate_table_from_csv(spark, s3_path)

	spark.stop()

if __name__ == '__main__':
	
	main()

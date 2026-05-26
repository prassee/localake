# Overview
This repository provides a fully self-contained local Apache Iceberg lab environment for simulating and studying the small-files problem at scale.
The project intentionally creates thousands of tiny Iceberg data files within a single timestamp-based partition to reproduce real-world operational issues commonly seen in:
- Streaming ingestion pipelines
- CDC workloads
- Event-driven architectures
- Micro-batch ETL systems
- Poorly optimized Iceberg write strategies
The environment is designed for:
- Iceberg experimentation
- Metadata stress testing
- Compaction benchmarking
- Query planning analysis
- Spark + Iceberg operational learning
- Trino interoperability testing
The entire platform runs locally using Docker Compose.

## Platform architecture

The stack consists of the following components:

| Component      | Purpose                          |
|---------------:|:---------------------------------|
| MinIO          | S3-compatible object storage     |
| Nessie         | Iceberg REST catalog             |
| Spark Master   | Distributed Spark coordinator    |
| Spark Worker 1 | PySpark execution worker         |
| Spark Worker 2 | PySpark execution worker         |
| Trino          | Interactive SQL query engine     |

### High-level data flow
```
              +----------------------+
              |        Trino         |
              | SQL Query Engine     |
              +----------+-----------+
                     |
                     v
              +----------------------+
              |        Nessie        |
              | Iceberg REST Catalog |
              +----------+-----------+
                     |
         +-----------------+-----------------+
         |                                   |
         v                                   v
+----------------------------+      +----------------------------+
|       Spark Cluster        |      |           MinIO            |
|  PySpark ETL + Compaction  | ---> |  S3-Compatible Warehouse   |
+----------------------------+      +----------------------------+

      Spark Master
         /    \
        /      \
   Spark Worker  Spark Worker
       1             2
```
## Project goals
The primary goals of this repository are:
- Create Iceberg tables backed by MinIO object storage
- Use Nessie as the Iceberg REST catalog
- Generate thousands of tiny Parquet files intentionally
- Concentrate files into a single timestamp partition
- Observe metadata growth and query planning degradation
- Benchmark Iceberg compaction procedures
- Validate interoperability between Spark, Nessie, MinIO, and Trino
- Provide a reproducible local learning environment
## Docker Compose environment
The environment is expected to be deployed using Docker Compose. All services must communicate over a shared Docker network.

### Infrastructure components

#### 1. MinIO

MinIO acts as the S3-compatible object storage layer.

**Responsibilities**

- Store Iceberg data files
- Store metadata and manifest files
- Provide S3 APIs for Spark and Trino
- Simulate cloud object storage locally

**Recommended bucket**: `warehouse`

**Recommended ports**

| Service       | Port |
|---------------|------:|
| MinIO API     | 9000 |
| MinIO Console | 9001 |

**Recommended credentials**

```
MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
```

#### 2. Nessie

Nessie acts as the Iceberg REST catalog.

**Responsibilities**

- Iceberg table metadata management
- Snapshot tracking
- Branching and tagging support
- Catalog version management

**Recommended API endpoint**: `http://nessie:19120/api/v1`

**Recommended port**

| Service    | Port  |
|-----------:|------:|
| Nessie API | 19120 |

#### 3. Spark cluster

The Spark cluster provides distributed ETL and compaction execution.

**Topology**

| Component     | Count |
|--------------:|------:|
| Spark Master  | 1     |
| Spark Workers | 2     |

**Responsibilities**

- Execute PySpark ETL jobs
- Generate small Iceberg files
- Execute Iceberg maintenance procedures
- Simulate distributed workloads
- Benchmark query planning overhead

**Recommended ports**

| Component             | Port |
|----------------------:|------:|
| Spark Master UI       | 8080 |
| Spark Worker 1 UI     | 8081 |
| Spark Worker 2 UI     | 8082 |
| Spark Master RPC      | 7077 |

**Spark requirements**

- Iceberg Spark extensions enabled
- S3A filesystem configured
- Nessie REST catalog configured
- Hadoop AWS libraries installed
- PySpark support enabled

#### 4. Trino

Trino provides interactive SQL access to Iceberg tables.

**Responsibilities**

- Interactive SQL querying
- Metadata inspection
- Query performance validation
- Iceberg interoperability testing

**Recommended port**

| Service          | Port |
|-----------------:|------:|
| Trino Coordinator| 8088 |

**Trino requirements**

- Iceberg connector enabled
- Nessie catalog integration
- MinIO S3 endpoint configuration
- Path-style S3 access enabled
## Iceberg table design

**Table name**: `demo.db.small_file_test`

**Schema**

| Column   | Type      |
|:---------|:----------|
| id       | BIGINT    |
| event_ts | TIMESTAMP |
| payload  | STRING    |

**Partition strategy**

The table is partitioned using:

```
PARTITIONED BY (days(event_ts))
```

This intentionally forces:

- Hot partitions
- File concentration
- Metadata amplification
- Compaction pressure
### Small-file simulation strategy

The project intentionally creates pathological write patterns.

**Key characteristics**

- Very small batch sizes
- Frequent commits
- Thousands of tiny Parquet files
- Single partition concentration
- No automatic compaction

This reproduces operational anti-patterns commonly observed in poorly optimized streaming systems.

**Example workload configuration**

Recommended defaults:

```
TOTAL_FILES = 5000
ROWS_PER_FILE = 1
```

Expected result:

- Thousands of tiny Parquet files
- Large manifest growth
- Large snapshot lineage
- Query planning degradation

### Spark session requirements

The Spark session must:

- Use Iceberg Spark extensions
- Connect to Nessie REST catalog
- Use MinIO via S3A
- Support distributed execution

Example:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("iceberg-small-files-lab")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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
```
## Examples

### Iceberg table creation

```sql
CREATE TABLE IF NOT EXISTS nessie.db.small_file_test (
    id BIGINT,
    event_ts TIMESTAMP,
    payload STRING
)
USING iceberg
PARTITIONED BY (days(event_ts))
```

### Small file generator (PySpark)

```python
from pyspark.sql.functions import lit, expr

TOTAL_FILES = 5000
ROWS_PER_FILE = 1

for batch_id in range(TOTAL_FILES):
    df = (
        spark.range(ROWS_PER_FILE)
        .withColumn(
            "event_ts",
            lit("2026-05-01 10:15:00").cast("timestamp")
        )
        .withColumn("payload", expr(f"'batch_{batch_id}'"))
    )

    (
        df
        .repartition(1)
        .writeTo("nessie.db.small_file_test")
        .append()
    )
```
### Iceberg validation queries

- Count data files

```sql
SELECT count(*)
FROM nessie.db.small_file_test.files
```

- Inspect partition distribution

```sql
SELECT
        partition,
        count(*) AS file_count,
        sum(file_size_in_bytes) AS total_size
FROM nessie.db.small_file_test.files
GROUP BY partition
```

- Inspect snapshots

```sql
SELECT *
FROM nessie.db.small_file_test.snapshots
```

- Inspect manifests

```sql
SELECT *
FROM nessie.db.small_file_test.manifests
```

### Iceberg compaction

The environment is intended to benchmark Iceberg compaction procedures.

Example:

```sql
CALL nessie.system.rewrite_data_files(
    table => 'db.small_file_test',
    strategy => 'binpack',
    options => map(
        'target-file-size-bytes', '134217728'
    )
)
```

**Target file size**: 128 MB = 134217728 bytes

### Recommended experiments

#### Query planning analysis

```python
spark.sql(
        "SELECT count(*) FROM nessie.db.small_file_test"
).explain(True)
```

Observe:

- Scan planning latency
- Task explosion
- Metadata loading overhead
- Driver memory pressure

Measure before vs after compaction:

- Query latency
- File count
- Manifest count
- Snapshot count
- Planning time
- Shuffle size

#### Streaming simulation

Use repeated micro-batches to emulate Kafka ingestion, CDC pipelines, Structured Streaming jobs, and high-frequency commits.

Example:

```
MICRO_BATCHES = 1000
ROWS_PER_BATCH = 50
```
### Operational anti-patterns being simulated

This repository intentionally reproduces bad operational patterns.

Examples:

- Tiny file writes
- Frequent commits
- Hot partitions
- Excessive repartitioning
- Missing compaction
- Snapshot accumulation

These patterns should not be used in production environments.

### Production recommendations

For real Iceberg deployments:

- Target 128 MB–512 MB file sizes
- Compact tables periodically
- Batch streaming commits
- Avoid excessive partitioning
- Monitor manifest growth
- Expire old snapshots
- Remove orphan files
- Tune distribution mode
- Monitor object store request rates
- Cleanup

Example cleanup:

```sql
DROP TABLE nessie.db.small_file_test PURGE
```

## Contributor guidelines

Contributors should:

- Keep workloads reproducible
- Preserve deterministic partitioning
- Document Spark configuration changes
- Avoid random partition spread
- Benchmark before optimization
- Preserve interoperability across Spark and Trino

## Success criteria

The simulation is considered successful when:

- Thousands of tiny files exist
- Most files are under a few KB
- All files are concentrated in one partition
- Query planning latency becomes noticeable
- Manifest growth becomes observable
- Compaction significantly reduces file counts
- Spark and Trino can both query the same Iceberg tables
Overview

This repository provides a fully self-contained local Apache Iceberg lab environment for simulating and studying the small-files problem at scale.

The project intentionally creates thousands of tiny Iceberg data files within a single timestamp-based partition to reproduce real-world operational issues commonly seen in:

Streaming ingestion pipelines
CDC workloads
Event-driven architectures
Micro-batch ETL systems
Poorly optimized Iceberg write strategies

The environment is designed for:

Iceberg experimentation
Metadata stress testing
Compaction benchmarking
Query planning analysis
Spark + Iceberg operational learning
Trino interoperability testing

The entire platform runs locally using Docker Compose.

Platform Architecture

The stack consists of the following components:

Component	Purpose
MinIO	S3-compatible object storage
Nessie	Iceberg REST catalog
Spark Master	Distributed Spark cluster coordinator
Spark Worker 1	PySpark execution worker
Spark Worker 2	PySpark execution worker
Trino	Interactive SQL query engine
High-Level Data Flow
                    +----------------------+
                    |        Trino         |
                    | SQL Query Engine     |
                    +----------+-----------+
                               |
                               v
                    +----------------------+
                    |        Nessie        |
                    | Iceberg REST Catalog |
                    +----------+-----------+
                               |
             +-----------------+-----------------+
             |                                   |
             v                                   v
+----------------------------+      +----------------------------+
|       Spark Cluster        |      |           MinIO            |
|  PySpark ETL + Compaction  | ---> |  S3-Compatible Warehouse   |
+----------------------------+      +----------------------------+


        Spark Master
           /    \
          /      \
   Spark Worker  Spark Worker
         1             2
Project Goals

The primary goals of this repository are:

Create Iceberg tables backed by MinIO object storage
Use Nessie as the Iceberg REST catalog
Generate thousands of tiny Parquet files intentionally
Concentrate files into a single timestamp partition
Observe metadata growth and query planning degradation
Benchmark Iceberg compaction procedures
Validate interoperability between Spark, Nessie, MinIO, and Trino
Provide a reproducible local learning environment
Docker Compose Environment

The environment is expected to be deployed using Docker Compose.

All services must communicate over a shared Docker network.

Infrastructure Components
1. MinIO

MinIO acts as the S3-compatible object storage layer.

Responsibilities:

Store Iceberg data files
Store metadata and manifest files
Provide S3 APIs for Spark and Trino
Simulate cloud object storage locally

Recommended bucket:

warehouse

Recommended ports:

Service	Port
MinIO API	9000
MinIO Console	9001

Recommended credentials:

MINIO_ROOT_USER=minio
MINIO_ROOT_PASSWORD=minio123
2. Nessie

Nessie acts as the Iceberg REST catalog.

Responsibilities:

Iceberg table metadata management
Snapshot tracking
Branching and tagging support
Catalog version management

Recommended API endpoint:

http://nessie:19120/api/v1

Recommended port:

Service	Port
Nessie API	19120
3. Spark Cluster

The Spark cluster provides distributed ETL and compaction execution.

Topology:

Component	Count
Spark Master	1
Spark Workers	2

Responsibilities:

Execute PySpark ETL jobs
Generate small Iceberg files
Execute Iceberg maintenance procedures
Simulate distributed workloads
Benchmark query planning overhead

Recommended ports:

Component	Port
Spark Master UI	8080
Spark Worker 1 UI	8081
Spark Worker 2 UI	8082
Spark Master RPC	7077

Spark requirements:

Iceberg Spark extensions enabled
S3A filesystem configured
Nessie REST catalog configured
Hadoop AWS libraries installed
PySpark support enabled
4. Trino

Trino provides interactive SQL access to Iceberg tables.

Responsibilities:

Interactive SQL querying
Metadata inspection
Query performance validation
Iceberg interoperability testing

Recommended port:

Service	Port
Trino Coordinator	8088

Trino requirements:

Iceberg connector enabled
Nessie catalog integration
MinIO S3 endpoint configuration
Path-style S3 access enabled
Iceberg Table Design
Table Name
demo.db.small_file_test
Schema
Column	Type
id	BIGINT
event_ts	TIMESTAMP
payload	STRING
Partition Strategy

The table is partitioned using:

PARTITIONED BY (days(event_ts))

This intentionally forces:

Hot partitions
File concentration
Metadata amplification
Compaction pressure
Small File Simulation Strategy

The project intentionally creates pathological write patterns.

Key characteristics:

Very small batch sizes
Frequent commits
Thousands of tiny Parquet files
Single partition concentration
No automatic compaction

This reproduces operational anti-patterns commonly observed in poorly optimized streaming systems.

Example Workload Configuration

Recommended defaults:

TOTAL_FILES = 5000
ROWS_PER_FILE = 1

Expected result:

Thousands of tiny Parquet files
Large manifest growth
Large snapshot lineage
Query planning degradation
Spark Session Requirements

The Spark session must:

Use Iceberg Spark extensions
Connect to Nessie REST catalog
Use MinIO via S3A
Support distributed execution

Example:

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("iceberg-small-files-lab")
    .config(
        "spark.sql.extensions",
        "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
    )
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
Example Iceberg Table Creation
CREATE TABLE IF NOT EXISTS nessie.db.small_file_test (
    id BIGINT,
    event_ts TIMESTAMP,
    payload STRING
)
USING iceberg
PARTITIONED BY (days(event_ts))
Example Small File Generator
from pyspark.sql.functions import lit, expr


TOTAL_FILES = 5000
ROWS_PER_FILE = 1


for batch_id in range(TOTAL_FILES):


    df = (
        spark.range(ROWS_PER_FILE)
        .withColumn(
            "event_ts",
            lit("2026-05-01 10:15:00").cast("timestamp")
        )
        .withColumn("payload", expr(f"'batch_{batch_id}'"))
    )


    (
        df
        .repartition(1)
        .writeTo("nessie.db.small_file_test")
        .append()
    )
Iceberg Validation Queries
Count Data Files
SELECT count(*)
FROM nessie.db.small_file_test.files
Inspect Partition Distribution
SELECT
    partition,
    count(*) AS file_count,
    sum(file_size_in_bytes) AS total_size
FROM nessie.db.small_file_test.files
GROUP BY partition
Inspect Snapshots
SELECT *
FROM nessie.db.small_file_test.snapshots
Inspect Manifests
SELECT *
FROM nessie.db.small_file_test.manifests
Iceberg Compaction

The environment is intended to benchmark Iceberg compaction procedures.

Example:

CALL nessie.system.rewrite_data_files(
  table => 'db.small_file_test',
  strategy => 'binpack',
  options => map(
    'target-file-size-bytes', '134217728'
  )
)

Target file size:

128 MB = 134217728 bytes
Recommended Experiments
Query Planning Analysis
spark.sql(
    "SELECT count(*) FROM nessie.db.small_file_test"
).explain(True)

Observe:

Scan planning latency
Task explosion
Metadata loading overhead
Driver memory pressure
Before vs After Compaction

Measure:

Query latency
File count
Manifest count
Snapshot count
Planning time
Shuffle size
Streaming Simulation

Use repeated micro-batches to emulate:

Kafka ingestion
CDC pipelines
Structured Streaming jobs
High-frequency commits

Example:

MICRO_BATCHES = 1000
ROWS_PER_BATCH = 50
Operational Anti-Patterns Being Simulated

This repository intentionally reproduces bad operational patterns.

Examples:

Tiny file writes
Frequent commits
Hot partitions
Excessive repartitioning
Missing compaction
Snapshot accumulation

These patterns should not be used in production environments.

Production Recommendations

For real Iceberg deployments:

Target 128 MB–512 MB file sizes
Compact tables periodically
Batch streaming commits
Avoid excessive partitioning
Monitor manifest growth
Expire old snapshots
Remove orphan files
Tune distribution mode
Monitor object store request rates
Cleanup

Example cleanup:

DROP TABLE nessie.db.small_file_test PURGE
Contributor Guidelines

Contributors should:

Keep workloads reproducible
Preserve deterministic partitioning
Document Spark configuration changes
Avoid random partition spread
Benchmark before optimization
Preserve interoperability across Spark and Trino
Success Criteria

The simulation is considered successful when:

Thousands of tiny files exist
Most files are under a few KB
All files are concentrated in one partition
Query planning latency becomes noticeable
Manifest growth becomes observable
Compaction significantly reduces file counts
Spark and Trino can both query the same Iceberg tables
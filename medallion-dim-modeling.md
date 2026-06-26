# Medallion Architecture — Silver Layer Modeling Guide

Stack: MinIO (S3) + Nessie (catalog) + Apache Iceberg + Spark 3.5 + Trino 481, bronze tables synced from PostgreSQL via Olake CDC.

---

## Layer Structure

```
bronze (nessie.db.*)          ← Olake CDC dumps from PgSQL, raw/unmodified
silver (nessie.silver.*)      ← Cleaned facts, SCD dims, cumulative snapshots
gold   (nessie.gold.*)        ← Aggregated marts for BI
```

---

## Table Types in Silver

| Type | Tracks | Source | Grain |
|------|--------|--------|-------|
| Fact | Immutable business events | Event/transaction bronze tables | 1 row per event |
| SCD Type 2 | Descriptive attribute changes over time | Entity/reference bronze tables | 1 row per entity version |
| Cumulative | Metrics that accumulate over time | Fact tables | 1 row per entity per day |

---

## 1. Fact Table

Facts are **immutable and append-only** once written. Use MERGE only for deduplication or late-arriving data.

```python
# etl/src/silver_fact_upi_transactions.py
ICEBERG_TABLE = "nessie.silver.fact_upi_transactions"

spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.fact_upi_transactions (
        transaction_sk  BIGINT,
        id              BIGINT,
        event_ts        TIMESTAMP,
        event_date      DATE,
        user_id         STRING,
        upi_id          STRING,
        event_type      STRING,
        bank            STRING,
        amount          DOUBLE,
        status          STRING,
        device_id       STRING,
        _loaded_at      TIMESTAMP
    )
    USING iceberg
    PARTITIONED BY (event_date)
    TBLPROPERTIES (
        'write.merge.mode' = 'merge-on-read',
        'write.parquet.compression-codec' = 'snappy'
    )
""")

spark.sql("""
    MERGE INTO nessie.silver.fact_upi_transactions t
    USING (
        SELECT
            abs(hash(id, event_ts))   AS transaction_sk,
            id,
            event_ts,
            CAST(event_ts AS DATE)    AS event_date,
            user_id,
            upi_id,
            upper(trim(event_type))   AS event_type,
            upper(trim(bank))         AS bank,
            amount,
            upper(trim(status))       AS status,
            device_id,
            current_timestamp()       AS _loaded_at
        FROM nessie.db.upi_events
        WHERE event_ts >= (SELECT coalesce(max(event_ts), TIMESTAMP '2000-01-01')
                           FROM nessie.silver.fact_upi_transactions)
    ) s
    ON t.id = s.id AND t.event_ts = s.event_ts
    WHEN NOT MATCHED THEN INSERT *
""")
```

**Design notes:**
- Partition by `event_date` (DATE) rather than `days(event_ts)` — Trino partition pruning works cleanly on `WHERE event_date = ...`.
- Use `merge-on-read` for fast writes on silver; flip to `copy-on-write` on gold where reads dominate.

---

## 2. SCD Type 2 Dimension

SCD2 tracks **what a record looked like at any point in time**. Each attribute change closes the old row and opens a new one.

### Schema convention

| Column | Purpose |
|--------|---------|
| `user_sk` | Surrogate key — hash of business key + attribute hash |
| `user_id` | Natural/business key from source |
| `eff_from` | When this version became active |
| `eff_to` | When it expired (`9999-12-31` = still current) |
| `is_current` | Shortcut for `eff_to = 9999-12-31` |
| `_checksum` | Hash of tracked columns — change detection signal |

```python
# etl/src/silver_dim_users.py
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.dim_users (
        user_sk     BIGINT,
        user_id     STRING,
        name        STRING,
        email       STRING,
        phone       STRING,
        kyc_status  STRING,
        eff_from    TIMESTAMP,
        eff_to      TIMESTAMP,
        is_current  BOOLEAN,
        _checksum   STRING
    )
    USING iceberg
    PARTITIONED BY (is_current)
    TBLPROPERTIES ('write.merge.mode' = 'merge-on-read')
""")

# Step 1: expire rows whose attributes changed
spark.sql("""
    MERGE INTO nessie.silver.dim_users t
    USING (
        SELECT
            user_id,
            name, email, phone, kyc_status,
            md5(concat_ws('|', name, email, phone, kyc_status)) AS _checksum
        FROM nessie.db.users
    ) s
    ON t.user_id = s.user_id
       AND t.is_current = true
       AND t._checksum != s._checksum
    WHEN MATCHED THEN UPDATE SET
        eff_to      = current_timestamp(),
        is_current  = false
""")

# Step 2: insert new version for changed + brand-new records
spark.sql("""
    INSERT INTO nessie.silver.dim_users
    SELECT
        abs(hash(s.user_id, s._checksum))  AS user_sk,
        s.user_id,
        s.name, s.email, s.phone, s.kyc_status,
        current_timestamp()                AS eff_from,
        TIMESTAMP '9999-12-31 00:00:00'    AS eff_to,
        true                               AS is_current,
        s._checksum
    FROM (
        SELECT
            user_id, name, email, phone, kyc_status,
            md5(concat_ws('|', name, email, phone, kyc_status)) AS _checksum
        FROM nessie.db.users
    ) s
    LEFT JOIN nessie.silver.dim_users t
        ON t.user_id = s.user_id AND t.is_current = true
    WHERE t.user_id IS NULL
       OR t._checksum != s._checksum
""")
```

**Point-in-time lookup:**
```sql
SELECT * FROM nessie.silver.dim_users
WHERE user_id = 'u123'
  AND eff_from <= TIMESTAMP '2025-01-15 00:00:00'
  AND eff_to   >  TIMESTAMP '2025-01-15 00:00:00'
```

---

## 3. Cumulative Table

A cumulative table is a **daily snapshot that carries ALL prior history forward**. Each row represents one entity on one day with lifetime and recent-window metrics. It grows by exactly one partition per day and old partitions are never rewritten.

```python
# etl/src/silver_cumulative_user_metrics.py
spark.sql("""
    CREATE TABLE IF NOT EXISTS nessie.silver.cumulative_user_metrics (
        snapshot_date       DATE,
        user_id             STRING,
        ltv_amount          DOUBLE,
        total_txns          BIGINT,
        txns_last_7d        BIGINT,
        txns_last_30d       BIGINT,
        amount_last_7d      DOUBLE,
        amount_last_30d     DOUBLE,
        first_txn_date      DATE,
        last_txn_date       DATE,
        is_active_last_30d  BOOLEAN
    )
    USING iceberg
    PARTITIONED BY (snapshot_date)
""")

# Run daily — inserts exactly ONE new partition for today
spark.sql("""
    INSERT INTO nessie.silver.cumulative_user_metrics
    SELECT
        CURRENT_DATE                              AS snapshot_date,
        f.user_id,
        sum(f.amount)                             AS ltv_amount,
        count(*)                                  AS total_txns,
        count_if(f.event_date >= CURRENT_DATE - INTERVAL '7'  DAY) AS txns_last_7d,
        count_if(f.event_date >= CURRENT_DATE - INTERVAL '30' DAY) AS txns_last_30d,
        sum(CASE WHEN f.event_date >= CURRENT_DATE - INTERVAL '7'  DAY THEN f.amount END) AS amount_last_7d,
        sum(CASE WHEN f.event_date >= CURRENT_DATE - INTERVAL '30' DAY THEN f.amount END) AS amount_last_30d,
        min(f.event_date)                         AS first_txn_date,
        max(f.event_date)                         AS last_txn_date,
        max(f.event_date) >= CURRENT_DATE - INTERVAL '30' DAY AS is_active_last_30d
    FROM nessie.silver.fact_upi_transactions f
    WHERE f.status = 'SUCCESS'
    GROUP BY f.user_id
""")
```

**Why this is valuable:** historical metric lookups are O(1) — `WHERE snapshot_date = '2025-03-15'` hits a single partition. No recomputation from raw events.

---

## Why SCD2 Should NOT Be Built from Cumulative Tables

A natural question: can we derive SCD2 by diffing consecutive daily snapshots of a cumulative table?

### The core mismatch

SCD2 and cumulative tables draw from **different source tables** and model fundamentally different things:

- Cumulative tables are built from **event/fact tables** (transactions, clicks). They carry metrics.
- SCD2 dimensions are built from **entity/reference tables** (users, merchants, products). They carry descriptive attributes like `name`, `email`, `kyc_status`.

There is nothing in `cumulative_user_metrics` to derive `email` or `kyc_status` from. The sources are independent.

### What breaks if you force it

If you load descriptive columns into the cumulative table to enable diffing:

```
snapshot_date | user_id | name  | kyc_status | total_txns
2025-01-01    | u123    | Alice | PENDING    | 5
2025-01-02    | u123    | Alice | VERIFIED   | 7
```

**Day-boundary precision loss.** If a user's status changed twice in one day, you only see the final state at midnight. Olake CDC gives you the exact change timestamp — discarding it to infer changes from daily snapshots is a step backward.

**Backfill cascade.** Rerunning the cumulative table for a bug fix or late data forces a full re-derivation of SCD2 history. The dimension loses independent trustworthiness.

**O(days × entities) diff on every run.** Instead of merging only the changed rows that CDC already flagged.

**Conceptual pollution.** A cumulative table that also carries descriptive attributes for diffing is neither a clean metrics snapshot nor a proper SCD2. Both become harder to reason about and optimize.

### The correct dependency graph

```
Olake CDC (entity tables: users, merchants)
    └─→  silver.dim_users               (SCD2 MERGE — owns attribute history)

Olake CDC (event tables: upi_events)
    └─→  silver.fact_upi_transactions   (incremental fact)
              └─→  silver.cumulative_user_metrics  (daily snapshot of metrics)
```

Dimensions and cumulative tables are **parallel tracks**, not a chain.

### The one valid exception

If the source has **no CDC** — only nightly full dumps with no change timestamps — diffing consecutive snapshots to detect changes is the right fallback. That pattern is called snapshot-based SCD2. With Olake providing CDC, you already have the change signal; using it directly is strictly better.

---

## Streaming Architecture

### Agreed cadence

```
bronze          every 1 min    Olake CDC → Iceberg (continuous micro-batch)
dim SCD2        every 15 min   foreachBatch MERGE
fact            every 17 min   foreachBatch MERGE  ← 2-min offset, runs after dims finish
running totals  every 15 min   OVERWRITE today's partition
cumulative      daily 00:05    INSERT one new snapshot_date partition
```

The 2-minute offset between dims and facts within each silver cycle ensures surrogate keys are resolved correctly. Running both at the same wall-clock time risks facts landing `-1` surrogate keys because dims haven't committed yet.

### Bronze: snapshot accumulation

At 1-minute intervals, each bronze table produces 1,440 Iceberg snapshots per day. Configure expiry to prevent unbounded metadata growth:

```sql
ALTER TABLE nessie.db.upi_events
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '86400000',
    'history.expire.min-snapshots-to-keep' = '5'
);
```

Also schedule a daily compaction job — 1,440 small parquet files per partition will degrade query performance without it.

### Silver facts: `foreachBatch` streaming

```python
# etl/src/stream_fact_upi_transactions.py
stream = (
    spark.readStream
    .format("iceberg")
    .load("nessie.db.upi_events")
)

def process_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("incoming_events")
    spark.sql("""
        MERGE INTO nessie.silver.fact_upi_transactions t
        USING (
            SELECT
                abs(hash(id, event_ts))   AS transaction_sk,
                id, event_ts,
                CAST(event_ts AS DATE)    AS event_date,
                user_id, upi_id,
                upper(trim(event_type))   AS event_type,
                upper(trim(bank))         AS bank,
                amount,
                upper(trim(status))       AS status,
                device_id,
                current_timestamp()       AS _loaded_at
            FROM incoming_events
        ) s
        ON t.id = s.id AND t.event_ts = s.event_ts
        WHEN NOT MATCHED THEN INSERT *
    """)

(stream.writeStream
    .foreachBatch(process_batch)
    .trigger(processingTime="17 minutes")
    .option("checkpointLocation", "s3a://warehouse/checkpoints/fact_upi/")
    .start()
    .awaitTermination())
```

The checkpoint in MinIO tracks which Iceberg snapshots have been consumed, guaranteeing exactly-once progress across restarts.

### Silver dims: `foreachBatch` streaming

SCD2 MERGE is stateful (requires reading current dim state before deciding what to do), so it cannot be expressed as a pure streaming transformation. `foreachBatch` gives you batch MERGE semantics inside a streaming job.

```python
# etl/src/stream_dim_users.py
stream = (
    spark.readStream
    .format("iceberg")
    .load("nessie.db.users")
)

def process_dim_batch(batch_df, batch_id):
    batch_df.createOrReplaceTempView("incoming_users")
    # Step 1: expire changed rows
    spark.sql("""
        MERGE INTO nessie.silver.dim_users t
        USING (
            SELECT *, md5(concat_ws('|', name, email, phone, kyc_status)) AS _checksum
            FROM incoming_users
        ) s
        ON t.user_id = s.user_id AND t.is_current = true AND t._checksum != s._checksum
        WHEN MATCHED THEN UPDATE SET
            eff_to = current_timestamp(), is_current = false
    """)
    # Step 2: insert new versions
    spark.sql("""
        INSERT INTO nessie.silver.dim_users
        SELECT abs(hash(s.user_id, s._checksum)) AS user_sk,
               s.user_id, s.name, s.email, s.phone, s.kyc_status,
               current_timestamp()                AS eff_from,
               TIMESTAMP '9999-12-31 00:00:00'    AS eff_to,
               true                               AS is_current,
               s._checksum
        FROM (SELECT *, md5(concat_ws('|', name, email, phone, kyc_status)) AS _checksum
              FROM incoming_users) s
        LEFT JOIN nessie.silver.dim_users t
            ON t.user_id = s.user_id AND t.is_current = true
        WHERE t.user_id IS NULL OR t._checksum != s._checksum
    """)

(stream.writeStream
    .foreachBatch(process_dim_batch)
    .trigger(processingTime="15 minutes")
    .option("checkpointLocation", "s3a://warehouse/checkpoints/dim_users/")
    .start())
```

### Late-arriving dimension: compensating MERGE

When facts run at T+17 and dims at T+15, there is still a window where a new entity arrives in the fact micro-batch before the dim has synced. The `coalesce(sk, -1)` sentinel handles this, but requires a compensating pass to back-fill resolved keys after the dim catches up:

```sql
-- run once after each dim refresh cycle
MERGE INTO nessie.silver.fact_upi_transactions t
USING (
    SELECT f.id, f.event_ts, u.user_sk
    FROM nessie.silver.fact_upi_transactions f
    JOIN nessie.silver.dim_users u
        ON u.user_id = f.user_id AND u.is_current = true
    WHERE f.user_sk = -1
) s
ON t.id = s.id AND t.event_ts = s.event_ts
WHEN MATCHED THEN UPDATE SET t.user_sk = s.user_sk
```

### Cumulative vs running totals

Running the cumulative table every 15 minutes breaks its semantic contract — the `snapshot_date` partition would keep changing throughout the day, losing its immutability guarantee. The solution is two separate tables with different jobs:

| Table | Refresh | Semantics |
|-------|---------|-----------|
| `silver.running_user_metrics` | Every 15 min, OVERWRITE today | Live intraday state — answers "what are metrics right now?" |
| `silver.cumulative_user_metrics` | Daily at 00:05, INSERT only | Immutable history — answers "what were metrics on day X?" |

```python
# etl/src/stream_running_user_metrics.py — overwrites today's partition each cycle
def process_running_batch(batch_df, batch_id):
    spark.sql("""
        INSERT OVERWRITE nessie.silver.running_user_metrics
        SELECT
            CURRENT_DATE        AS as_of_date,
            CURRENT_TIMESTAMP   AS refreshed_at,
            user_id,
            sum(amount)         AS ltv_amount,
            count(*)            AS total_txns,
            count_if(event_date >= CURRENT_DATE - INTERVAL '7'  DAY) AS txns_last_7d,
            count_if(event_date >= CURRENT_DATE - INTERVAL '30' DAY) AS txns_last_30d
        FROM nessie.silver.fact_upi_transactions
        WHERE status = 'SUCCESS'
        GROUP BY user_id
    """)
```

---

## Orchestration Order

```
1. [bronze]  Olake CDC sync       → nessie.db.*                         (every 1 min)
2. [silver]  SCD2 dims            → stream_dim_users.py                 (every 15 min)
3. [silver]  Facts                → stream_fact_upi_transactions.py     (every 17 min, after dims)
4. [silver]  Running totals       → stream_running_user_metrics.py      (every 15 min)
5. [silver]  Compensating MERGE   → back-fill -1 surrogate keys         (after each dim cycle)
6. [silver]  Cumulative snapshot  → silver_cumulative_user_metrics.py   (daily at 00:05)
```

---

## Nessie Branch Strategy

Test transforms without touching `main`:

```python
# Point Spark at a dev branch
.config("spark.sql.catalog.nessie.ref", "silver-dev")

# Trino: switch branches per query
USE nessie."silver-dev".silver;
```

Merge into `main` once validated — zero-copy, Git-style promotion.

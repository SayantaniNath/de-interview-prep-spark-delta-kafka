# PySpark / Spark — Q&A Log

Running log of questions asked during learning sessions, with answers. Newest date at bottom. Refresh anytime.

2026-06-02 — Stage 2B, PySpark SQL (Lesson 3)

Refresh me on lazy vs wide (transformations).

"Wide vs lazy" are two different axes, not opposites.

**Axis 1 — Lazy vs eager (WHEN work happens)**

  * **Transformations are lazy** — `select`, `filter`, `withColumn`, `groupBy`, `join`, `spark.sql("SELECT…")` just record a recipe (build the DAG). Nothing runs.
  * **Actions are eager** — `show()`, `count()`, `collect()`, `write` — trigger the whole recipe to execute.
  * Payoff: Spark sees the full plan before running, so Catalyst optimizes across steps (push filters down, pick join strategy).



**Axis 2 — Narrow vs wide (HOW MUCH data moves across the network)**

  * **Narrow** — each input partition feeds one output partition. No network movement. `select`, `filter`, `withColumn`. Cheap.
  * **Wide** — output partitions pull from many input partitions → **shuffle** (data sent across executors). `groupBy`, `join` (non-broadcast), `orderBy`, `distinct`. Expensive — the #1 thing you tune.
  * A **stage boundary** is exactly where a shuffle happens. Count shuffles → count stages.



**How they combine in Lesson 3:**

Operation| Lazy/eager| Narrow/wide  
---|---|---  
`SELECT … WHERE`| lazy until `.show()`| narrow  
`GROUP BY`| lazy until `.show()`| **wide** (shuffle)  
`ORDER BY`| lazy until `.show()`| **wide** (shuffle)  
`JOIN`| lazy until `.show()`| **wide** — unless broadcast (tiny `coin_meta` → BroadcastHashJoin, no shuffle)  
  
Full reference: `~/Downloads/Spark_Architecture_Walkthrough.html`

* * *

What does this do? `spark = (SparkSession.builder.appName("…").getOrCreate())`

It creates (or reuses) the SparkSession — the entry point every DataFrame/SQL call goes through.
    
    
    spark = (
        SparkSession.builder              # a configurator to set up a session
        .appName("FinFlow_Lesson3_SQL")   # name shown in Spark UI (localhost:4040)
        .getOrCreate()                    # build it — or hand back the existing one
    )

  * **`SparkSession.builder`** — a configurator you chain settings onto (app name, configs, master URL). Nothing exists yet.
  * **`.appName(...)`** — labels the application; purely for identification in the Spark UI and logs.
  * **`.getOrCreate()`** — the actual build. "Get" the active session if one exists in this process, else "create" one. A JVM allows only one active session, so re-running won't spin up duplicates or error.



**What you get back (`spark`):** the handle that owns `spark.read.json(...)`, `spark.sql("…")`, and launches the Driver + Spark UI on `localhost:4040`. This `spark` object _is_ the Driver's entry point — builds the DAG, hands tasks to executors. Creating it is setup, not an action — nothing computes here.

The outer parentheses are just Python letting you split a chained call across lines for readability — no Spark meaning.

* * *

Why is the path wrapped in `str(...)`? `DATA = str(Path.home() / "Downloads" / … / "sample.jsonl")`

Because `Path.home() / "…"` produces a PosixPath object, and Spark's reader wants a plain **string**.

  * **`Path.home()`** → a `Path` object (`/Users/sayantaninath`).
  * **`/ "Downloads" / …`** → the `/` operator is overloaded on `Path` to join segments. Still a `Path`, not text. (Readable, OS-agnostic joins — the nice part of `pathlib`.)
  * **`str(...)`** → flattens it into the real string `"/Users/sayantaninath/Downloads/…/sample.jsonl"`.



**Why Spark needs the string:** `spark.read.json(path)` hands the path **across to the JVM** (Spark's engine is Scala/Java). The JVM doesn't understand a Python `PosixPath` — it expects a Java string. Pure-Python libs like `pandas.read_json` accept a `Path` directly (they honor `os.PathLike`); the Python→JVM boundary doesn't, so you convert explicitly. Skip the `str()` and you'd likely hit a `py4j` type error.

**Pattern:** use `pathlib` to build the path readably, wrap in `str()` at the moment you hand it to Spark.
    
    
    p = Path.home() / "Downloads"
    print(type(p))        # <class 'pathlib.PosixPath'>
    print(type(str(p)))   # <class 'str'>

* * *

What's the alternative to `show(n)` to show ALL records?

`df.show()` defaults to 20 rows and truncates long columns at 20 chars. To show all:
    
    
    df.show(df.count())                 # pass the exact row count as n
    df.show(df.count(), truncate=False) # + full untruncated column values

Method| Returns| When to use  
---|---|---  
`df.show(df.count())`| prints, nothing returned| quick visual scan of all rows  
`df.collect()`| Python list of Row objects| pull data to driver to loop/process  
`df.toPandas()`| pandas DataFrame| nicer display (notebooks), pandas ops  
  
⚠️ The catch: all three pull every row to the Driver — an **action** that can blow up driver memory or hang on a big table. Fine on 6 rows, dangerous in production. `df.show(20)` exists so you peek cheaply without dragging the whole dataset back.

`truncate=False` is the flag you'll reach for most — keeps timestamps and names from being cut off.

* * *

Difference between using the DataFrame API and Spark SQL?

Two front-ends to the same engine — no performance difference. Both compile through the same Catalyst optimizer to the same physical plan.
    
    
    # DataFrame API
    df.select("coin", "price_usd").filter(F.col("price_usd") > 1000)
    
    # Spark SQL — identical result, identical plan
    spark.sql("SELECT coin, price_usd FROM prices WHERE price_usd > 1000")

Run `.explain()` on both → same physical plan (that's Ex 10 in Lesson 3).

**Why have both? About how you write/read code, not speed:**

| DataFrame API| Spark SQL  
---|---|---  
Looks like| Python method chaining| SQL strings  
Errors caught| compile/build time (bad method name fails immediately)| runtime (bad SQL fails only when query runs)  
Composability| great — build `df` step by step, reuse, pass to functions| weaker — string concatenation gets ugly  
Complex logic readability| nested chains get hard to read| complex joins/aggs often read cleaner  
Who reaches for it| Python engineers, programmatic pipelines| analysts, SQL-first, ad-hoc queries  
IDE support| autocomplete, type hints| just a string — no help  
  
**Guidance:** DataFrame API for dynamically-built logic (loops/conditionals/reusable fns) or compile-time safety; Spark SQL for gnarly multi-join/aggregation queries that read cleaner as SQL or SQL-fluent collaborators. **Mix freely** — `createOrReplaceTempView` flips a DataFrame into SQL and back (common: heavy transforms in the API, final aggregation in SQL).

Interview takeaway: "Same engine, same Catalyst plan, same performance — choice is readability, composability, and compile-time vs runtime error checking." Proving equivalence with `.explain()` is what they're testing.

* * *

What is Iceberg, Hudi, Trino?

Iceberg & Hudi are table formats; Trino is a query engine — different layers of the stack.

**The problem they exist for:** a raw data lake is just Parquet files in object storage (S3). Files alone give no transactions, no time travel, no safe concurrent writes. The lakehouse stack fixes that.

**Table formats — Iceberg & Hudi (and Delta)** — a metadata layer on top of Parquet files that turns a pile of files into a real table with: ACID transactions, time travel (query old snapshots), schema evolution, and MERGE/upserts/deletes.

Format| Born at| Sweet spot| Ecosystem  
---|---|---|---  
Delta| Databricks| tight Databricks/Spark integration| Databricks  
Iceberg| Netflix| huge tables, engine-neutral, hidden partitioning| Apple, Netflix, Snowflake, AWS — momentum winner  
Hudi| Uber| streaming upserts, CDC, incremental pulls| Uber, Robinhood  
  
Same problem; differences are partition handling, how updates are stored, and which engines/vendors back them.

**Trino** — a distributed SQL query engine (formerly PrestoSQL), _not_ a storage format. Doesn't store data; runs fast SQL _across_ data wherever it lives. Superpower: **federation** — one query can join S3 (Iceberg/Hudi/Delta) + PostgreSQL + MySQL. "The SQL brain you point at many sources," vs Spark which is more a general compute/ETL engine.

Interview takeaway: table format (Delta/Iceberg/Hudi) = ACID + time travel on lake files; query engine (Trino, Spark SQL, Athena) = runs SQL over them. "Why Iceberg?" → engine-neutral + scales to massive tables + industry momentum. "Why Delta?" → best if all-in on Databricks.

## show() vs display()

Q: What is the difference between `show()` and `display()` in Spark?

**Both show data from a DataFrame — but they work in different environments.**

`show()` — PySpark, works anywhere
    
    
    df.show()                  # first 20 rows (default)
    df.show(5)                 # first 5 rows
    df.show(truncate=False)    # don't cut off long strings

Output is plain text in the terminal or console.

`display()` — Databricks notebooks only
    
    
    display(df)

Output is a rich interactive table — sortable columns, filter, chart view (bar/line/pie), download as CSV. Not available outside Databricks.

| show()| display()  
---|---|---  
Works in| Anywhere (terminal, script, notebook)| Databricks notebooks only  
Output| Plain text table| Interactive UI table  
Charts| ❌ No| ✅ Yes — built-in  
Truncates long strings| Yes (use truncate=False to disable)| No — shows full content  
Use in production scripts| ✅ Yes| ❌ No  
  
Rule: In Databricks → `display()`. In a local script or terminal → `show()`.

## PySpark vs Flink vs Presto/Trino — when to use which

Q: When do you use PySpark vs Flink vs Presto/Trino?

| PySpark| Flink| Presto/Trino  
---|---|---|---  
Best for| Batch ETL, ML, large-scale transforms| True real-time streaming (sub-second)| Fast ad-hoc SQL on existing data  
Processing model| Micro-batch or batch| Continuous, event-by-event| Query engine only — no pipeline  
Latency| Seconds to minutes| Milliseconds to seconds| Seconds (query time)  
Stores data?| No| No| No — queries data where it lives  
Typical use| DW pipelines, Delta Lake, feature engineering| Fraud detection, real-time alerts, CDC| BI dashboards, cross-source federation  
  
**PySpark** — data is large, you need transformations + joins + aggregations, seconds of latency is fine. 90% of DE work.

**Flink** — true sub-second real-time needed (fraud scoring per transaction as it happens). More operationally complex than Spark Streaming.

**Presto/Trino** — data already exists in S3/Delta/Iceberg/Postgres and you want fast SQL without moving it. Superpower: federation — one query across multiple sources.

Interview one-liner: PySpark = batch/streaming ETL engine. Flink = true real-time streaming engine. Presto/Trino = fast SQL query engine across existing data stores. They solve different problems and often coexist in the same stack.

---

## Delta Lake — OCC (Optimistic Concurrency Control)

**Q: Two writers start at the same time. Writer A commits first. Does Writer B retry automatically or throw ConcurrentModificationException?**

**Delta throws — it does NOT retry automatically.**

Delta uses optimistic concurrency: writers don't lock the table upfront. When B tries to commit, Delta checks: do B's read/write files overlap with what A just modified?

| Scenario | Result |
|---|---|
| B's files overlap with A's changed files | Delta throws `ConcurrentModificationException` — your code must retry |
| B's files don't overlap (e.g. different partitions) | B's commit succeeds — both writers coexist |

**Key point:** Retry is your application's responsibility. Delta only detects and reports the conflict — it does not auto-retry.

**Common wrong answer:** "B retries automatically." Delta throws; you retry.

---

## Delta Lake — VACUUM Retention Horizon

**Q: A table has commits 0–25. You run VACUUM with default settings. What is the earliest version Delta keeps data files for?**

**VACUUM is time-based, not commit-count-based.**

It removes Parquet data files no longer referenced by any version within the last 7 days (default). Commit numbers don't matter — only timestamps do.

- If all 26 commits happened in the last 3 days → VACUUM removes nothing.
- If commits 0–10 happened 2 weeks ago → those versions' Parquet files may be removed.

**What VACUUM touches:** Parquet data files only. The `_delta_log` is untouched. Time travel uses the log to reconstruct versions — but if the Parquet files have been vacuumed, the read will fail.

**Common wrong answer:** "Earliest version is commit 20" — you can't derive a version number from a retention window without knowing timestamps.

---

## Delta Lake — VACUUM Commands (all variants)

**Q: What are the VACUUM command variants in Delta Lake?**

```sql
-- Default (7-day retention)
VACUUM my_table

-- Custom retention
VACUUM my_table RETAIN 336 HOURS       -- 14 days
VACUUM my_table RETAIN 168 HOURS       -- 7 days (explicit)

-- Dry run — shows what WOULD be deleted, deletes nothing
VACUUM my_table DRY RUN

-- Go below 7 days (disables safety check — breaks time travel)
SET spark.databricks.delta.retentionDurationCheck.enabled = false;
VACUUM my_table RETAIN 0 HOURS;
```

**Rule:** Always run `DRY RUN` first on production tables. Never go below 7 days unless you explicitly don't need time travel.

---

## Delta Lake — MERGE Write Amplification

**Q: You MERGE 1,000 rows into a Delta table with 500 Parquet files. Only 20 files contain matching rows. How many files are rewritten? Why is this a problem at scale?**

**All 20 files are rewritten in full — even if each file had only 1 matching row.**

Parquet files are immutable. Delta cannot update a single row in-place. For every file that contains at least one match, Delta must:
1. Read the entire file
2. Apply the change to the matching rows
3. Write a brand new Parquet file
4. Mark the old file as deleted in `_delta_log`

The other 480 files are untouched.

**The scale problem:** If those 20 files are 1 GB each → 20 GB of I/O for a 1,000-row change.

| Mitigation | Why it helps |
|---|---|
| Partition on merge key | Delta prunes to relevant partitions — far fewer files touched |
| Z-ORDER on merge key | Co-locates matching rows into fewer files |
| OPTIMIZE before large MERGE | Compaction → fewer files → less file-open overhead |

**Interview one-liner:** MERGE rewrites every touched file in full because Parquet is immutable. Partition + Z-ORDER on your merge key to minimize how many files get touched.

---

## Delta Lake — Delta Sharing

**Q: What problem does Delta Sharing solve, and how does it work?**

**Problem:** Sharing a Delta table across teams, companies, or clouds — without copying data, without giving access to your cloud storage, and without requiring the recipient to use Databricks.

**How it works:** The data owner runs a Delta Sharing server (or uses Databricks). The recipient gets a credential file (short-lived token + server URL) and queries the shared table through any Delta Sharing client (Python, Spark, pandas, Power BI). Data stays in the owner's storage — recipients get pre-signed URLs to specific files only.

| | Copy data | Delta Sharing |
|---|---|---|
| Data freshness | Stale immediately | Always live |
| Storage cost | 2x | Owner pays once |
| Access control | Recipient owns copy | Owner revokes any time |
| Recipient needs Databricks | No | No |

**Interview one-liner:** Delta Sharing is an open protocol for sharing live Delta tables across clouds and organizations without copying data or requiring the recipient to use Databricks.

---

## Delta vs Iceberg vs Hudi

**Q: What problem do all three solve that plain Parquet on S3 doesn't?**

Plain Parquet has no transaction layer — no ACID, no updates/deletes, no history, no schema enforcement. All three add a transaction log on top of Parquet to solve this.

**Q: What are the key differences between Delta, Iceberg, and Hudi?**

| | Delta Lake | Apache Iceberg | Apache Hudi |
|---|---|---|---|
| Transaction log | `_delta_log/` — JSON + Parquet checkpoints | metadata/ — Avro manifest files + snapshot pointers | `.hoodie/` — timeline of commits |
| Created by | Databricks | Netflix | Uber |
| Ecosystem | Databricks-native | Neutral — AWS/Google prefer it | Streaming-heavy workloads |
| Streaming strength | Structured Streaming + Auto Loader | Good, not native | Built for near-real-time upserts |
| Read engines | Spark, Trino, Flink, DuckDB | Spark, Trino, Flink, Athena, BigQuery | Spark, Flink, Presto |

**Ecosystem split:**
- **Delta** — on Databricks, or tightest Spark integration needed
- **Iceberg** — on AWS (Athena, Glue) or GCP, or engine-neutral open standard; Apple uses it at massive scale
- **Hudi** — high-frequency upserts with low write latency (Uber's use case: trip updates every few seconds)

**Log difference in one sentence:** Iceberg uses Avro manifest files + snapshot pointers in a metadata folder; Delta uses JSON commit files + Parquet checkpoints in `_delta_log/`.

**Interview one-liner:** All three add ACID on top of Parquet. Delta wins on Databricks. Iceberg wins on AWS/GCP and multi-engine shops. Hudi wins for high-frequency streaming upserts.

---

## Spark Cluster Sizing & Partitioning — Classic Interview Question

**Q: 5 executors, 4 cores each, 16 GB memory each. Process a 300 GB dataset. How do you plan partitioning, parallelism, memory, and performance tuning?**

### Step 1 — Resources
```
5 × 4 cores  = 20 total task slots
5 × 16 GB    = 80 GB total memory
Dataset      = 300 GB
```

### Step 2 — Input partitioning
Target 128–200 MB per partition (`maxPartitionBytes = 128 MB`).
```
300 GB / 128 MB = ~2,400 input partitions
2,400 / 20 cores = 120 task waves  ← healthy
```
```python
spark.conf.set("spark.sql.files.maxPartitionBytes", "134217728")
```

### Step 3 — Shuffle partitions
Default 200 → 1.5 GB per partition → too large, will spill. Fix: target 200 MB → 1,500 partitions.
```python
spark.conf.set("spark.sql.shuffle.partitions", "1500")
# Or with AQE (Spark 3+):
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "2000")  # AQE coalesces automatically
```

### Step 4 — Memory breakdown per executor
| Layer | Amount |
|---|---|
| Total heap | 16 GB |
| Reserved (hardcoded) | 300 MB |
| Usable heap | ~15.7 GB |
| Unified pool (execution + storage) — 60% | ~9.4 GB |
| User memory (UDFs) — 40% | ~6.3 GB |
| **Per-task unified memory (÷ 4 cores)** | **~2.35 GB/task** |

```python
spark.conf.set("spark.executor.memoryOverhead", "2g")  # prevent container OOM
```

### Step 5 — Tuning checklist
| Lever | When | Action |
|---|---|---|
| Broadcast join | Small table < 10 MB | `autoBroadcastJoinThreshold = 10485760` |
| AQE skew join | Max task >> median in Spark UI | `spark.sql.adaptive.skewJoin.enabled = true` |
| Salting | groupBy skew or Spark 2.x | See salting section below |
| Kryo | Always | `spark.serializer = KryoSerializer` |
| Caching | Dataset reused 2+ times | Cache aggregations only — 300 GB won't fit in 80 GB |

**Interview answer:** "With 20 cores I'd target 2,400 input partitions at 128 MB — 120 task waves. For shuffles I'd set 1,500 partitions to stay at ~200 MB each, or enable AQE to tune dynamically. Each task gets ~2.35 GB of unified memory — enough headroom for 128 MB partitions without spilling. I'd monitor Spark UI for skew and spill, broadcast small tables, and enable AQE skew join as a safety net."

---

## Data Skew — Salting Pattern

**Q: What is data skew, how do you detect it, and how does salting fix it?**

### What is skew?
One partition holds far more rows than others — usually a hot key in a join or groupBy (e.g. "unknown" customer_id = 40% of rows). One task takes 10× longer; the whole stage waits.

### How to detect
- Spark UI → Stages → Tasks → sort by Duration: max >> median = skew
- Shuffle Read Size: Min/Median vs Max — a 16× ratio is textbook skew
- Many empty partitions (median = 0 B) — data piled into a few buckets

```python
from pyspark.sql.functions import spark_partition_id, count

df.groupBy(spark_partition_id().alias("partition_id")) \
  .agg(count("*").alias("row_count")) \
  .orderBy("row_count", ascending=False) \
  .show(20)
```

### Salting — join skew fix
```python
from pyspark.sql.functions import col, floor, rand, lit, explode, array, concat

SALT_BUCKETS = 9  # ceil(max_partition_rows / target) = ceil(41468 / 5000)

# Skewed side: append random salt 0..8
skewed_df = skewed_df.withColumn(
    "salted_key",
    concat(col("customer_id"), lit("_"), (floor(rand() * SALT_BUCKETS)).cast("int"))
)

# Other side: replicate N times with each salt value
other_df = other_df.withColumn(
    "salted_key",
    explode(array([concat(col("customer_id"), lit(f"_{i}")) for i in range(SALT_BUCKETS)]))
)

result = skewed_df.join(other_df, "salted_key")
```

### Salting — groupBy skew fix (two-pass)
```python
# Pass 1 — partial aggregation with salt
partial = df.withColumn("salt", (floor(rand() * SALT_BUCKETS)).cast("int")) \
            .groupBy("customer_id", "salt") \
            .agg(sum("amount").alias("partial_sum"))

# Pass 2 — final aggregation drops salt
result = partial.groupBy("customer_id").agg(sum("partial_sum").alias("total_amount"))
```

### SALT_BUCKETS formula
```
SALT_BUCKETS = ceil(max_partition_rows / target_rows_per_partition)
             = ceil(41468 / 5000) = 9
```

### AQE vs manual salting
| | AQE Skew Join | Manual Salting |
|---|---|---|
| Effort | Zero — enable config | Code change required |
| Works for | Joins only (Spark 3+) | Joins + groupBy |
| Use when | Spark 3+, standard join skew | Spark 2.x, groupBy skew, AQE not available |

**Interview one-liner:** "Salting distributes a hot key across N partitions by appending a random integer — the other side is replicated N times to match. For Spark 3+ I enable AQE first; salting is the fallback for groupBy skew or older clusters."

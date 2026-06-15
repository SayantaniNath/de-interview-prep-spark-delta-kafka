# DE Interview Prep — Full Learning Path
**Sayantani Nath | Updated: 2026-06-15**
**Goal:** Job-ready for FAANG / Databricks DE roles by Oct 1, 2026
**Base:** Strong SQL, Python, Snowflake/Redshift production, AWS CCP in progress

---

## STATUS KEY
- ✅ Complete
- 🟡 In Progress
- ⏳ Not Started
- ⏹️ Skipped / Replaced

---

# PHASE 1 — DE-Focused (Jun 8 → Oct 1, 2026)

Structured by interview round order — the thing tested first is built first.

---

## PILLAR 1: SQL + Python Coding
*Interview round: CodeSignal (Round 1) + Algorithm Onsite (Round 2)*

### SQL — Ongoing throughout Phase 1

| Topic | Status | Notes |
|---|---|---|
| Window functions: RANK, DENSE_RANK, ROW_NUMBER, LAG, LEAD, NTILE, PERCENT_RANK | ✅ | Done — dailysql.com 2026-05-27 |
| Complex GROUP BY, HAVING, CTEs, set operations | ✅ | |
| JOIN types, NULL handling, conversion math | ✅ | |
| SCD Type 2 with Delta MERGE INTO syntax | ⏳ | Covered in Pillar 2C |
| DataLemur — 2 problems/week (Tue/Thu 20 min) | 🟡 | Ongoing through Oct |

### Python Algorithms — NeetCode 150 (expanded scope for Databricks)
*Cadence: Mon/Wed, 25 min. Log: ~/Downloads/interview_prep/python_log.md*

| Pattern | ~Problems | Est. Window | Status |
|---|---|---|---|
| Arrays & Hashing | Two Sum ✅ + Group Anagrams ✅ + Top K Frequent, Contains Duplicate, Valid Anagram, Product Except Self, Encode/Decode, Longest Consecutive (~8 total) | Jun 8 → Jun 30 | 🟡 |
| Two Pointers | Valid Palindrome, Two Sum II, 3Sum, Container With Most Water, Trapping Rain Water (~5) | Jul 1 → Jul 14 | ⏳ |
| Sliding Window | Best Time to Buy/Sell, Longest Substring Without Repeating, Longest Repeating Char, Permutation in String (~4) | Jul 14 → Jul 28 | ⏳ |
| Trees BFS/DFS ← Databricks CodeSignal | Level Order Traversal, Max Depth, Same Tree, Invert Tree, Validate BST, Right Side View (~6) | Jul 28 → Aug 11 | ⏳ |
| Linked Lists ← Databricks CodeSignal | Reverse LL, Merge Two Sorted, Palindrome LL, LRU Cache (~4) | Aug 11 → Aug 25 | ⏳ |
| Binary Search ← Databricks CodeSignal | Capacity to Ship Packages, Find Min in Rotated, Search in Rotated (~3) | Aug 25 → Sep 1 | ⏳ |

---

## PILLAR 2: Spark + Databricks Technical Depth
*Interview round: Spark/Databricks Tech Fit (Round 4) — biggest differentiator*
*Slot: Mon/Wed/Fri 1 hr. Est. window: Jun 8 → Jul 18*

### 2A: Spark Architecture
*Started: 2026-05-28 | Completed: 2026-06-03*

| Topic | Status |
|---|---|
| Driver / Executor / Cluster Manager / SparkSession | ✅ |
| Partitions as unit of parallelism | ✅ |
| Transformations vs actions, lazy evaluation | ✅ |
| Narrow vs wide dependencies, shuffles, stage boundaries | ✅ |
| Job / Stage / Task hierarchy, DAG | ✅ |
| Broadcast joins: auto (<10MB), explicit hint, 50MB boundary | ✅ |
| Spark UI: live session, BroadcastHashJoin confirmed hands-on | ✅ |
| DataFrame API Lessons 1+2: select/filter/join/withColumn/groupBy/agg/orderBy | ✅ |
| PySpark SQL Lesson 3: createOrReplaceTempView, spark.sql, Catalyst plan reading | ✅ |
| Databricks CE live session: serverless compute, display(), Unity Catalog volume upload | ✅ |

### 2B: Spark Internals — Deep Dive
*Started: 2026-06-07 | Completed: 2026-06-09*

| Topic | Status |
|---|---|
| Catalyst optimizer: 4 phases (Analysis → Logical Opt → Physical Planning → Code Gen) | ✅ |
| Tungsten: off-heap memory, whole-stage code generation, WSCG `*(1)` in explain output | ✅ |
| AQE: partition coalescing, dynamic join strategy switching, skew join optimization (Spark 3.0+) | ✅ |
| Shuffles: map-side write → network → reduce side, stage barrier | ✅ |
| spark.sql.shuffle.partitions: why 200 is wrong, how to tune | ✅ |
| Join strategies: BHJ / SMJ / SHJ — when each fires, stage count, speed | ✅ |
| Caching: storage levels, lazy gotcha, when to cache vs not, unpersist | ✅ |
| repartition vs coalesce: shuffle vs no-shuffle, when to use each | ✅ |
| Salting — conceptual + full walkthrough doc (join salting, groupBy salting, Spark UI detection) | ✅ |
| Spark UI hands-on + salting from-blank exercises | ✅ 2026-06-08 |
| Photon engine: C++ vectorized columnar execution, when it activates vs JVM Spark | ✅ 2026-06-09 |

> **2B fully complete. Next: 2C Delta Lake.**

### 2C: Delta Lake Mechanics ⭐ (most-asked topic in Databricks DE interviews)
*Actual start: 2026-06-12 | Est. completion: 2026-06-27*

| Topic | Status |
|---|---|
| _delta_log structure: JSON commit files + Parquet checkpoints at every 10th commit | ✅ 2026-06-12 |
| ACID via optimistic concurrency control (writer conflict resolution) | ✅ 2026-06-12 |
| Time travel: how Delta reconstructs snapshot at version N using the log | ✅ 2026-06-12 (incl. RESTORE) |
| Schema enforcement vs evolution (mergeSchema option), risks of each | ✅ 2026-06-12 |
| OPTIMIZE + Z-ORDER: when it helps, when it doesn't, column selection strategy | ✅ 2026-06-12 (incl. liquid clustering) |
| VACUUM: default 7-day retention, time travel impact, why to never drop below retention period | ✅ 2026-06-12 |
| MERGE INTO: isolation level, under-the-hood mechanics, upsert patterns | ✅ 2026-06-12 |
| Change Data Feed (CDF): pre-image/post-image for UPDATE, how to enable and consume | ✅ 2026-06-12 |
| Small files problem: API call math, how OPTIMIZE consolidates | ✅ 2026-06-12 |
| Dynamic file pruning + data skipping (min/max column statistics) | ✅ 2026-06-12 |
| Delta Sharing: what problem it solves, how it differs from copying data | ✅ 2026-06-15 |
| Delta vs Iceberg vs Hudi: transaction log architecture differences, ecosystem split (Databricks/Apple/Uber) | ✅ 2026-06-15 |
| Hands-on lab: build Delta table in CE, inspect _delta_log, watch checkpoint at v10 (walkthrough doc §23) | ✅ 2026-06-15 |

### 2D: Structured Streaming ⭐
*Est. start: 2026-06-27 | Est. completion: 2026-07-07*

| Topic | Status |
|---|---|
| Checkpoint: what it stores (offsets + state), consequence of deleting it | ⏳ |
| Watermarking: late-arriving data, how window state is evicted after watermark passes | ⏳ |
| Output modes: append / update / complete — when each applies, constraints | ⏳ |
| Exactly-once with Kafka: idempotent producer + transactional API + Delta atomic writes | ⏳ |
| Atomic micro-batch commits to Delta Lake | ⏳ |
| Trigger modes: default (micro-batch), once, processingTime, availableNow | ⏳ |

### 2E: Delta Live Tables (DLT) ⭐
*Est. start: 2026-07-07 | Est. completion: 2026-07-11*

| Topic | Status |
|---|---|
| Lakeflow Designer intro (Andreas Kretz repo, done 2026-05-21) | ✅ |
| DLT vs manually writing Spark pipelines — what problem it solves | ✅ intro level |
| Expectations: warn / drop / fail modes, when to use each | ⏳ |
| Streaming live table vs materialized view | ⏳ |
| Schema drift prevention: StructType + Expectations pattern | ⏳ |
| Pipeline update modes: complete vs append | ⏳ |

### 2F: Unity Catalog ⭐
*Est. start: 2026-07-11 | Est. completion: 2026-07-14*

| Topic | Status |
|---|---|
| Three-level namespace: catalog.schema.table | ⏳ |
| RBAC: grants, roles, privilege inheritance | ⏳ |
| Row-level security + column masking for PII | ⏳ |
| Data lineage tracking — how it captures table-level and column-level lineage | ⏳ |
| Metastore vs legacy Hive metastore: what changes, what migrates | ⏳ |

### 2G: Auto Loader
*Est. start: 2026-07-14 | Est. completion: 2026-07-16*

| Topic | Status |
|---|---|
| Auto Loader vs COPY INTO: when to use which | ⏳ |
| Two file discovery modes: directory listing vs file notification (Event Grid/SNS) | ⏳ |
| Schema inference, evolution, and schema hints | ⏳ |
| cloudFiles format in PySpark Structured Streaming | ⏳ |

### 2H: MLflow Basics (appears in Databricks DE rounds)
*Est. start: 2026-07-16 | Est. completion: 2026-07-18*

| Topic | Status |
|---|---|
| Experiment tracking: runs, params, metrics, artifacts | ⏳ |
| Model registry: stages (Staging / Production / Archived) | ⏳ |
| Log model from PySpark/sklearn, load and serve a registered model | ⏳ |
| MLflow UI navigation — what interviewers expect you to know | ⏳ |

### 2I: Databricks Platform + Ecosystem
*Fold into above sessions throughout Jun–Jul*

| Topic | Status |
|---|---|
| All-Purpose Cluster vs Job Cluster: cost, startup time, use cases | ⏳ |
| DBUs and cluster sizing for cost estimation | ⏳ |
| Databricks Workflows vs Airflow: when to choose which | ⏳ |
| Secret scopes: Databricks-backed vs Azure Key Vault-backed | ⏳ |
| CI/CD: Databricks Repos + Git integration + databricks-cli | ⏳ |
| ai_query() / Mosaic AI functions: inline LLM calls in SQL/PySpark | ⏳ |
| Photon-enabled vs standard Databricks Runtime | ⏳ |
| **Databricks vs Snowflake vs BigQuery** — architecture-level articulation for interviews | ⏳ |

*Snowflake architecture comparison topics are covered as a full crash course — see **4C: Snowflake Crash Course** (Tue/Thu Sep 1–14).*

### 2J: Data Modeling
*Est. start: 2026-07-14 | Est. completion: 2026-07-18*

| Topic | Status |
|---|---|
| Star vs snowflake schema, fact vs dimension tables, grain definition | ⏳ |
| SCD Type 1 (overwrite), Type 2 (history rows with effective dates), Type 3 (prev-value column) | ⏳ |
| Implementing SCD Type 2 with Delta MERGE INTO — full from-scratch exercise | ⏳ |

---

## PILLAR 3: Concurrency Round
*Interview round: Dedicated Concurrency Round at Databricks (Round 3) — most candidates unprepared*
*Est. start: 2026-08-11 | Est. completion: 2026-08-18 | ~8 targeted problems, 1 week intensive*

| Topic | Status |
|---|---|
| Python threading primitives: Lock, RLock, Semaphore, Condition, Event | ⏳ |
| Thread-safe LRU cache with TTL eviction | ⏳ |
| Thread-safe logger with concurrent queue (deque + locking) | ⏳ |
| Sliding window rate limiter (deque + Python time module, 300-sec window) | ⏳ |
| Distributed rate limiter design: Redis + token bucket (conceptual + code skeleton) | ⏳ |
| Write-ahead logging + crash recovery on single-machine key-value store | ⏳ |
| ConcurrentHashMap internal locking — asked as follow-up in Java/conceptual form | ⏳ |

---

## PILLAR 4: Data Engineering Core
*Interview round: Tech Fit + System Design rounds*

### 4A: Kafka
*Est. start: 2026-07-21 | Est. completion: 2026-08-01 | Slot: Mon/Wed/Fri*

| Topic | Status |
|---|---|
| Core concepts: topics, partitions, producers, consumers, consumer groups, offsets | ⏳ |
| Exactly-once semantics: idempotent producer + transactional API | ⏳ |
| Partitioning strategy: key-based vs round-robin, when each matters | ⏳ |
| Replication factor and fault tolerance | ⏳ |
| Kafka Connect + Debezium basics (for CDC pipeline project) | ⏳ |
| Python kafka-python library — producers and consumers | ⏳ |
| Local dev: Redpanda via Docker (Kafka-API-compatible, lighter than Kafka) | ⏳ |

### 4B: Airflow
*Est. start: 2026-08-01 | Est. completion: 2026-08-11 | Slot: Mon/Wed/Fri + Tue/Thu*

| Topic | Status |
|---|---|
| DAGs, operators: BashOperator, PythonOperator, SparkSubmitOperator | ⏳ |
| Scheduling (cron), retries, SLAs | ⏳ |
| TaskGroup, XComs for inter-task data passing, sensors | ⏳ |
| Airflow vs Databricks Workflows trade-off articulation | ⏳ |
| Wire Airflow DAG into Fraud Detection Pipeline as orchestration layer | ⏳ |

### 4C: Snowflake Crash Course ⭐ (Tom Bailey Udemy)
*Slot: Tue/Thu 1 hr | Est. start: 2026-09-01 | Est. completion: 2026-09-14*
*Why: production background exists; crash course to solidify architecture vocabulary for "Databricks vs Snowflake" interview Q, and prerequisite for dbt (4D)*

| Topic | Status |
|---|---|
| Virtual Warehouses: compute/storage separation, auto-suspend/resume, multi-cluster | ⏳ |
| Micro-partitions: automatic clustering vs Delta Z-ORDER | ⏳ |
| Time Travel + Fail-Safe: retention periods, mechanics, how they differ from Delta | ⏳ |
| Zero-copy cloning vs Delta SHALLOW CLONE | ⏳ |
| Snowpipe (continuous ingestion) vs Auto Loader — trigger model comparison | ⏳ |
| Streams (CDC) vs Delta Change Data Feed | ⏳ |
| Tasks (scheduling) vs Airflow / Databricks Workflows | ⏳ |
| Data Sharing vs Delta Sharing | ⏳ |
| VARIANT, FLATTEN, PARSE_JSON — semi-structured data handling | ⏳ |
| RBAC: roles, users, privilege model | ⏳ |
| Result cache + query pruning: how Snowflake avoids recompute | ⏳ |
| Snowflake vs Databricks: when each wins (governance-heavy warehouse vs open lakehouse) | ⏳ |

### 4D: dbt Crash Course
*Slot: Tue/Thu 1 hr | Est. start: 2026-09-15 | Est. completion: 2026-09-30*
*Prerequisite: Snowflake (4C). Practice project connects dbt Core to Snowflake.*

| Topic | Status |
|---|---|
| Project structure: models/, seeds/, tests/, macros/, snapshots/ | ⏳ |
| Models: ref(), source(), materialization types (table, view, incremental, ephemeral) | ⏳ |
| Incremental models: is_incremental() macro, unique_key, merge vs append strategy | ⏳ |
| Schema tests: not_null, unique, accepted_values, relationships | ⏳ |
| Custom singular data tests | ⏳ |
| Snapshots: SCD Type 2 with dbt (timestamp strategy + check strategy) | ⏳ |
| Seeds: loading CSV reference data | ⏳ |
| Macros + Jinja templating | ⏳ |
| dbt docs generate + serve — lineage graph | ⏳ |
| CLI commands: dbt run, dbt test, dbt debug, dbt deps, dbt compile | ⏳ |
| profiles.yml: connecting dbt Core to Snowflake | ⏳ |
| dbt Cloud vs dbt Core trade-offs | ⏳ |
| Practice project: 3-layer model (staging → intermediate → mart) on Snowflake | ⏳ |

---

## PILLAR 5: System Design (FAANG-grade, all 4 phases)
*Interview round: System Design (Round 5)*

### Phase A — DDIA Reading (1 chapter/weekend)
*Est. start: 2026-05-23 | Phase A ends: Jun 14*

| Chapter | Status | Date |
|---|---|---|
| Ch 1 — Reliable, Scalable, Maintainable | ✅ | by 2026-05-29 |
| Ch 2 — Data Models and Query Languages | ✅ | by 2026-05-29 |
| Ch 3 — Storage and Retrieval | ✅ | 2026-06-01 |
| Ch 4 — Encoding and Evolution | ⏳ | Due Jun 20–21 (catch-up weekend) |
| Ch 5 — Replication | ⏳ | Due Jun 21–22 |
| Ch 6 — Partitioning | ⏳ | Due Jun 28–29 |
| Ch 7 — Transactions | ⏳ | Due Jul 5–6 |
| Ch 8 — Trouble with Distributed Systems | ⏳ | Due Jul 12–13 |
| Ch 9 — Consistency and Consensus | ⏳ | Due Jul 19–20 |

### Phase B — Case Studies (1/weekend, Jun 14 → Aug 31, ~10 total)
*6-step framework: requirements → estimation → API → schema → HLD → deep-dive*

| Case | Type | Status |
|---|---|---|
| Prepaid Credits / billing ledger (Chargebee-style) | DE/billing | ✅ 2026-06-01 |
| Design LeetCode (submission pipeline + SLA) | Classic | ✅ 2026-06-03 |
| Real-time fraud detection (Kafka → Structured Streaming → MLflow → Delta) | DE/streaming | ⏳ |
| Lakehouse medallion architecture with governance (Unity Catalog, lineage, RBAC) | DE/lakehouse | ⏳ |
| CDC pipeline at scale (Debezium → Kafka → Delta CDF, SCD Type 2) | DE/CDC | ⏳ |
| Streaming pipeline: late-data handling, watermarks, backpressure | DE/streaming | ⏳ |
| Multi-tenant feature store (offline + online, 50K QPS, sub-50ms p99) | ML/DE | ⏳ |
| Design the Delta Lake transaction protocol (the system itself) | Databricks-specific | ⏳ |
| Unity Catalog (multi-tenant metastore design) | Databricks-specific | ⏳ |
| RAG architecture (architectural only — no LangChain coding) | AI/DE | ⏳ |

### Phase C — Out-Loud Mocks (Sep 1 → Oct 15)
*2 mocks/week, Excalidraw + verbal walkthrough. Redo weak Phase B cases. Build behavioral stories in parallel.*

- ⏳ Mock 1–2 (Sep 1–5)
- ⏳ Mock 3–4 (Sep 7–12)
- ⏳ Mock 5–6 (Sep 14–19)
- ⏳ Mock 7–8 (Sep 21–26)

### Resources
- Designing Data-Intensive Applications — Kleppmann (Phase A–B)
- Alex Xu System Design Interview Vol 2 — Phase B (data-heavy systems)
- Alex Xu Machine Learning System Design Vol 3 — feature store, RAG, recommender chapters
- Hello Interview (hellointerview.com) ⭐ — FAANG-grade SD, Stefan Mai videos
- Exponent (tryexponent.com) — DE-specific mock interviews, Phase C
- excalidraw.com — whiteboarding

---

## PILLAR 6: Certifications

### AWS Cloud Practitioner (CLF-C02)
*Slot: Tue/Thu 1 hr | Target completion: early-mid Jul 2026 (ahead of schedule — Maarek finishing Jun 19)*

| Phase | Detail | Status |
|---|---|---|
| Skilljar Modules 1–5 | Modules 1-4 done; Module 5 Networking done 2026-05-19 | ✅ |
| Skilljar Module 6 | Skipped 2026-05-29 — Maarek covers same content faster | ⏹️ |
| Maarek Udemy course | Intensive week Jun 15-19 (1 hr Mon + 3 hrs/day Tue-Fri) — completing this week | 🟡 |
| Tutorials Dojo Round 1 | Full timed exam, review every wrong answer (~3 hrs) — starts Jun 22 | ⏳ |
| Patch weak topics | Re-watch Maarek sections exposed by TD | ⏳ |
| Tutorials Dojo Round 2 | Score 80%+ consistently → book real exam | ⏳ |

### Databricks Data Engineer Associate (DEA)
*Slot: Tue/Thu after AWS CCP completes (post Jul 18) | Target: Aug 2026*

| Resource | Status |
|---|---|
| Databricks Academy — "Data Engineering with Databricks" learning path | ⏳ |
| Derar Alhussein Udemy course (~6 hrs condensed) | ⏳ |
| Official Databricks practice exam | ⏳ |
| Skillcertpro / Udemy mocks | ⏳ |

---

## PILLAR 7: Projects (4 DE Portfolio Projects)
*Interview round: Project Walkthrough + demonstrates skills in every other round*

| Project | Stack | Est. Build Window | Status |
|---|---|---|---|
| **FinFlow — batch pipeline** | PySpark + Pandas + Delta Lake | Ongoing | 🟡 |
| **Fraud Detection Pipeline** ⭐ | Kafka → Structured Streaming → IsolationForest → alert Kafka topic → Delta Lake → Airflow | Aug 1–11 | ⏳ |
| **CDC Pipeline** | Debezium → Kafka → Delta (CDF enabled) + SCD Type 2 MERGE INTO | Aug 11–18 | ⏳ |
| **Databricks Lakehouse + AI Monitor** ⭐ | Bronze/Silver/Gold + DLT + Unity Catalog + Airflow + Claude API anomaly detection | Aug 18–29 | ⏳ |

**Portfolio polish window:** Sep 15 → Oct 1 — GitHub READMEs, architecture diagrams, measurable outcomes, deploy instructions.

**Resume note:** FinFlow is framed as a *fintech transactions pipeline* (PaySim synthetic payments data, NOT crypto). Repoint code from crypto JSONL → PaySim before any resume submission that names FinFlow as a payments/transactions project.

---

## PILLAR 8: Behavioral
*Interview round: Behavioral (Round 6) | Start: Aug 2026 — do NOT start early, stories get stale*

| Story | Status |
|---|---|
| Most complex data project — end-to-end ownership, trade-offs, measurable outcomes | ⏳ |
| Major optimization — latency / cost / reliability improvement with numbers | ⏳ |
| Conflict with a coworker + resolution | ⏳ |
| Decision made with incomplete information | ⏳ |
| Raised quality bar on a project | ⏳ |
| Why Databricks — must include real product opinion (Delta vs Iceberg, Photon, Unity Catalog model) | ⏳ |
| Databricks vs Snowflake vs BigQuery — architecture-level differentiation | ⏳ |
| Mistake owned + what changed afterward | ⏳ |

---

## PHASE 1 TIMELINE OVERVIEW

| Window | Primary Focus | Slot |
|---|---|---|
| Jun 8 → Jul 18 | Spark Internals (2B) → Delta Lake (2C) → Streaming (2D) → DLT (2E) → Unity Catalog (2F) → Auto Loader (2G) → MLflow (2H) → Data Modeling (2J) + Tom Bailey Snowflake (2I) + AWS CCP | Mon/Wed/Fri + Tue/Thu |
| Jul 18 → Aug 1 | Kafka (4A) + Databricks DEA cert starts | Mon/Wed/Fri |
| Aug 1 → Aug 11 | Airflow (4B) + wire Fraud Detection DAG | Mon/Wed/Fri + Tue/Thu |
| Aug 11 → Aug 18 | Concurrency round prep (Pillar 3) + CDC Pipeline build | Mon–Fri intensive |
| Aug 18 → Aug 29 | Databricks Lakehouse + AI Monitor capstone build + DEA cert push | Mon/Wed/Fri + Tue/Thu |
| Aug 29 → Sep 15 | SD out-loud mocks Phase C (2/week) + Behavioral stories | Flexible |
| Sep 1 → Sep 14 | Snowflake crash course (4C) — runs Tue/Thu parallel to SD mocks | Tue/Thu |
| Sep 15 → Sep 30 | dbt crash course (4D) — runs Tue/Thu parallel to portfolio polish | Tue/Thu |
| Sep 15 → Oct 1 | Portfolio polish + GitHub + Apply | Flexible |

**Ongoing every week:**
- Mon/Wed: NeetCode Python (25 min, within session)
- Tue/Thu: DataLemur SQL (20 min) + DDIA chapter (Thu 30–45 min)
- Weekend: SD Phase B case study (1/weekend, Jun 14 → Aug 31) + Databricks blog post (15 min)

---

# PHASE 2 — AI/LLM Engineering (post-October 2026)

Start after applying is underway. Not needed for DE interview rounds.

| Stage | Content | Status |
|---|---|---|
| Stage 5 — GraphQL | Queries, mutations, schema, playground | ⏳ Phase 2 |
| Stage 6 — DeepLearning.AI LLM Foundations | LangChain, LangGraph, ChatGPT Prompt Engineering, Building Systems with ChatGPT API | ⏳ Phase 2 |
| Stage 6 — Responsible / Ethical AI | Bias mitigation, risk assessment, quality reviews | ⏳ Phase 2 |
| Stage 7 — RAG Pipelines (hands-on) | LangChain RAG, Chroma vector DB, Resume Q&A Bot | ⏳ Phase 2 |
| Stage 8 — Agentic Workflows | LangGraph, CrewAI, Vertex Agent Builder, AI Job Application Agent project | ⏳ Phase 2 |
| GCP / Vertex AI full track | Vertex AI Workbench, Model Garden, Vector Search, Agent Builder, Pipelines | ⏳ Phase 2 |

**Note:** RAG *architecture* (for SD rounds) is covered in Phase 1 via a Phase B case study — no hands-on coding needed for interview purposes. LLM coding courses are deferred; architectural understanding is sufficient.

---

## Reference Files
- Spark walkthrough: `~/Downloads/My Learning Journey on DE/Spark_Architecture_Walkthrough.html`
- Databricks DEA resources: `~/Downloads/My Learning Journey on DE/Databricks_DEA_Learning_Resources.html`
- Databricks Lakehouse blueprint: `~/Downloads/My Learning Journey on DE/Project_Databricks_Lakehouse_Blueprint.html`
- Fraud Detection blueprint: `~/Downloads/My Learning Journey on DE/Project_Fraud_Detection_Blueprint.html`
- System Design Prepaid Credits: `~/Downloads/sysdesign/SystemDesign_Prepaid_Credits_Walkthrough.html`
- Python NeetCode log: `~/Downloads/My Learning Journey on DE/interview_prep/python_log.md`
- SD Q&A log: `~/Downloads/My Learning Journey on DE/interview_prep/sd_qa_log.md`

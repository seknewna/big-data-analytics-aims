# Glossary

Action
:   A Spark operation that triggers execution and returns a result or writes output (`count`, `collect`, `write`).

Adaptive Query Execution (AQE)
:   Spark feature that re-optimises a query at runtime using statistics from completed stages.

Amdahl's law
:   The maximum speed-up from parallelism is limited by the serial fraction of a program: \(S = 1/((1-p) + p/N)\).

Apache Arrow
:   A standard in-memory columnar data format enabling zero-copy data sharing between tools and languages.

Broadcast join
:   Join strategy where a small table is copied to every worker, avoiding a shuffle of the large table.

CAP theorem
:   During a network partition, a distributed data store must choose between consistency and availability.

Checkpoint
:   Saved state (offsets, aggregation state) that allows a streaming query to recover after failure; also used to truncate long lineage.

Combiner
:   A local reduce applied to map output before the shuffle to reduce data transfer.

Columnar storage
:   Storing each column contiguously; efficient for analytical queries that read few columns.

Data lake / Lakehouse
:   A data lake stores raw files in cheap object storage; a lakehouse adds a transaction log (Delta Lake, Iceberg, Hudi) for ACID tables on top.

DAG
:   Directed acyclic graph — the plan of tasks and dependencies built by Spark, Dask or an orchestrator.

Differential privacy
:   A mathematical privacy guarantee that an algorithm's output changes little whether or not any one person's data is included.

Driver / Executor
:   In Spark, the driver plans and schedules work; executors run tasks on partitions.

Event time / Processing time
:   When an event happened versus when the system processed it.

H3
:   A hierarchical hexagonal global grid system for spatial indexing.

Idempotent
:   An operation that gives the same result when repeated; essential for retries and backfills.

Lazy evaluation
:   Deferring computation until a result is required, allowing whole-plan optimisation.

Lineage
:   The record of transformations that produced a dataset; Spark uses it to recompute lost partitions.

MapReduce
:   A programming model where computation is expressed as map and reduce functions, with the framework handling distribution, shuffling and failures.

Mergeable statistic
:   A statistic whose partial results on subsets combine exactly into the result on the whole (sum, count, mean via sum and count, variance via Chan's formula).

Parquet
:   An open columnar file format with encoding, compression and per-chunk statistics.

Partition
:   (1) A chunk of a distributed dataset processed by one task. (2) A directory in a partitioned dataset (`year=2024/`). (3) A network split in the CAP theorem. (4) A shard of a Kafka topic.

Predicate / projection pushdown
:   Applying filters (predicates) and column selection (projection) at the data source to read less data.

Shuffle
:   Redistribution of data across workers by key; required for wide operations like groupBy and join.

Skew
:   Uneven distribution of data across keys/partitions, causing a few slow tasks.

Sketch
:   A compact probabilistic summary of a data stream (HyperLogLog, Count-Min, t-digest).

Transformation
:   A lazy Spark operation that defines a new dataset from an existing one (`select`, `filter`, `join`).

Watermark
:   A threshold on event time that tells a streaming engine how long to wait for late data before finalising results.

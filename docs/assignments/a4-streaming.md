# A4 — Streaming analytics

**Chapter:** 9 · **Dataset:** Wikimedia EventStreams (live) — or synthetic mobile money via Kafka · **Effort:** ~12 hours

## Goal

Build a fault-tolerant streaming application with event-time windows, watermarks and a reliable sink, and show you understand its guarantees.

## Tasks

1. **Ingestion (15%).** Bridge the live stream into Spark (file drop as in Chapter 9, or Kafka). Run for at least 30 minutes and keep the raw events.
2. **Windowed metrics (25%).** Every minute, for each wiki: number of edits, share by bots, approximate distinct editors (`approx_count_distinct`), and mean absolute size change (`length.new − length.old`, where available). Use event time with a watermark you justify.
3. **Alerting (20%).** Detect "edit bursts": a page with ≥ 5 edits from ≥ 3 distinct users within 10 minutes. Write alerts to a separate sink.
4. **Fault tolerance (20%).** Write raw events to Parquet (or Delta) with checkpointing. Kill the query mid-run and restart it. Prove there are no lost or duplicated events (e.g., by event `id` / `meta.id`).
5. **Sketches (10%).** Implement reservoir sampling and a Count-Min Sketch in Python on the raw events; compare the sketch's top-20 page estimates with exact counts.
6. **Report (10%).** Explain your output mode choices, watermark trade-offs, latency observed (`lastProgress`), and what would change for a production mobile money fraud system.

## Alternative: synthetic mobile money

Use the Faker generator from [Data sources](../resources/data-sources.md) with Kafka (docker compose in `labs/`). Inject delayed events (10% arrive 1–30 minutes late) and show how your watermark handles them.

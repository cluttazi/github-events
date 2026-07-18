# ADR 002 — Gold marts in PySpark; dbt-duckdb and Scala dropped

**Status**: accepted

## Context

The template's gold layer exists to demonstrate polyglot integration (a
Scala aggregation job, dbt-duckdb marts over Parquet exports). This project's
gold layer consumes PIT and bridge tables whose construction depends on the
shared DV2.0 hashing module.

## Decision

Build the information marts in PySpark, same engine as the vault loaders.

## Rationale

* The hash rules must exist exactly once (task requirement); a second engine
  would either re-implement them or export around them.
* The template CLAUDE.md itself flags its Python/Scala metrics lockstep as a
  standing hazard — we avoid creating a second such pair.
* The Databricks Asset Bundle chains python_wheel_tasks Bronze -> Gold with
  no engine boundary.

## Consequences

No `transform/dbt_project`, no `pipelines/gold_scala`, no Parquet export
step. If a SQL-first consumer interface is wanted later, virtualized views
over PIT + satellites are the natural addition.

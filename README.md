# calc-vp-staging

A Spark Scala application that loads Parquet datasets and persists them to Apache Iceberg tables using upsert (merge) semantics. Designed for staging dynamic feature data with evolving schemas.

## Overview

This project handles the incremental persistence of Parquet data to Iceberg, supporting:

- **Upsert via MERGE INTO**: New rows are inserted; existing rows (matched by business key) are updated
- **Schema evolution**: New columns in incoming batches are automatically added to the target table
- **Preserved values**: Columns present in the target but missing from a batch retain their existing values
- **Partitioning**: Tables are partitioned by `movimento` for efficient queries
- **Merge-on-read**: Optimized write properties minimize file rewrites during updates

## Tech Stack

| Component | Version |
| --------- | ------- |
| Java      | 11      |
| Scala     | 2.12.18 |
| Spark     | 3.5.3   |
| Iceberg   | 1.10.1  |
| Maven     | 3.x     |

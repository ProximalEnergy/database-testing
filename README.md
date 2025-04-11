# Database Comparison

I am looking to learn more about <database_name> and how it compares to Timescale Cloud. Can you give me a high level overview and feature comparison? I am particularly interested in the following.

- Ingest time: how quickly can I ingest data?
- Query time: how quickly can I query data?
- Storage: how much storage space does data use?
- Data Tiering: how does data tiering work? Are the options to move data to cheaper storage at the expense of query latency?
- Cost: how much does it cost to run a performance database?

I am looking to support ingestion rates of up to 10,000,000 rows per minute. Each of my data points have a timestamp, identifier, and a value. The value could be a float, integer, boolean, or string. I need to store this data for ~20 years.

It's also worth mentioning that I do not need to run typical analytical queries such as "find the average temperature grouped by hour of day". I do, however, need to be able to down sample data to even intervals. For example, I need to be able to query second by second data at 1 minute intervals. Being able to also store this sampled data at a lower resolution would be a plus.

Be sure to to describe things as they are named in the documentation so I can do further research if needed.

## Criteria

- Ingest time
- Query time
- Upsert time
- Data Tiering

## Comparison Table

| Criteria | ClickHouse | CrateDB | InfluxDB | QuestDB | TDEngine | Timescale |
|----------|------------|---------|----------|---------|----------|-----------|
| AWS Region Availability |  | No (US-East-1) |  |  |  |  |
| Current Hardware Configuration |  | 2 CPU, 2 GiB RAM, 8 GiB Storage |  |  |  |  |
| Preliminary Ingestion Rate |  | ~75,000 rows/s |  |  |  |  |

\* From local machine
## ClickHouse

### Notes from call

- Use SharedMergeTree
- Switch order of primary key (should be (tag_id, time))

### Questions

- When using Incremental Materialized Views, does the Materialized View *only* use new data to refresh the view? For example, let's say the goal of my Incremental Materialized View is to down sample data to 1 minute intervals. If I have already ingested 30 seconds worth of data and then later insert the rest of the 30 seconds worth of data (from that minute), will the Incremental Materialized View only use the new data to refresh the view? (https://clickhouse.com/docs/materialized-view/incremental-materialized-view#other-applications)
  - Incremental Materialized Views is exactly what I need!

- When using ClickHouse Cloud with object storage, are the buckets "owned" by ClickHouse. Or are they regular buckets in my object storage provider? (https://clickhouse.com/docs/cloud/reference/shared-merge-tree)

## CrateDB

### Questions

- Is there the option to deploy CrateDB Cloud in AWS US-EAST-2 (Ohio)?

### Notes from call

- "Continuous Aggregates" can be done in the Automation tab
- There is no true "seamless" query layer between the database and S3, this would need to be handled by us

## InfluxDB

### Notes from call

- Shared prices of 24k/year, 50k/year, 80k/year
- One organization
- Multiple buckets
- 50MB/s throughput is reasonable
- Global timeout of 10 minutes
- Serverless - all hot, Enterprise - Some cold

### Questions

- When I created an account I noticed the AWS US-EAST-2 (Ohio) region was not available. Is this correct?
- Should I be looking at the InfluxDB Cloud Dedicated documentation? I mostly work in Python, is there a client library that you would recommend?
  - Python client library for InfluxDB 3 (`influxdb3-python`) https://docs.influxdata.com/influxdb3/cloud-dedicated/reference/client-libraries/v3/python/
- The docs indicate that downsampling data looks like simply querying data from the database and inserting it into another database. Am I thinking about this correctly?
- I noticed a recommended batch size of 10,000 points. Does this sound about right?
  - At least 10,000 rows, maybe even 20, 30, 40k

## QuestDB

### Getting Started

- Run `docker run -p 9000:9000 -p 9009:9009 -p 8812:8812 -p 9003:9003 questdb/questdb:8.2.3`
- Open web UI at http://localhost:9000/

### Action Items

- Materialized Views (https://questdb.com/docs/concept/mat-views/)
- Upserting all of the data took a *very* long time on the largest data set

### Questions

- It appears that it is not possible to have a nullable boolean column. How have other customers handled this? Do they just use an integer type and cast boolean values to 0 or 1? (https://questdb.com/docs/reference/sql/datatypes/)

- The documentation recommends avoiding sparse tables. My current schema looks like (time, tag_id, value_int, value_float, value_bool, value_str) where any given time-tag_id pair will have a value in only one of the value_ columns. Do you have other customers using a similar approach? A wide schema (time, tag_1, tag_2, ..., tag_n) would definitely be more dense, but I'm not sure this can scale to 1000s of tags (especially from a query syntax perspective). (https://questdb.com/docs/guides/schema-design-essentials/#sparse-vs-dense-tables)

- The largest table I am creating (_5_minutes_of_100000_tags_at_1_second_intervals) is ~1.36 GiB. This is much larger than the same data set stored in a parquet file with no compression (~250 MB). Is this expected? I realize there is an option to enable compression with ZFS, but I haven't enabled this on my local machine. (https://questdb.com/docs/reference/function/meta/#table_storage)

- The largest table I am creating (_5_minutes_of_100000_tags_at_1_second_intervals) has 30,000,000 rows and is taking ~30 seconds to insert. Is this expected?

### Notes from call

- $800/month
- 4 core 32 GB per month

## TDEngine

## Timescale
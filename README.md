# Audicin_assignment

The stack I used in this assignment is dbt + Databricks + AWS S3. The code is orgnized to medallion layers, which orgnized as bronze (raw_data), silver (stg_data + int_data), gold (marts).

## Bronze Layer
This layer is the raw data from the datasets and is stored in the bronze schema in Databricks with delta format. The datasets used for this assignment include `events.ndjson`, `subscriptions.json`, `marketing_spend.csv`, and `exchange_rates.csv`. `exchange_rates.csv` file is not included in the given data, I searched it online to get the currency exchange rates of USD and NGN to EUR from 2026-01-01 to 2026-02-01, which is the time period of the events data. 

Since `dbt seed` can only load csv files from the dbt repo into Databricks, `marketing_spend.csv` and `exchange_rates.csv` are loaded using `dbt seed --select marketing_spend exchange_rates`. Loading `events.ndjson` into Databricks using following command:

```
create table events_json
using json
location 's3://audicin-assignment/bronze/events.ndjson';
```
Then convert the json format into delta format using following command:

```
CREATE OR REPLACE TABLE bronze.events
USING delta
AS
SELECT * FROM bronze.events_json;
```
Keep the json format that every query scans raw files, and it slows down the query performance, so I converted the json format into delta format.

The `subscriptions.json` is loaded using Databricks UI.


## Silver Layer
All the tables are structured incrementally and partitioned by date. I used the merge strategy to update the data. The data flow in Databricks schedules the jobs to run every day at 00:00.
### stg_events:
A cleaned version of the raw events data, with some basic data quality checks and transformations. I removed bad values, and nulls in event_id and user_id, added the event_date column. If the channel is null, I set it to 'unknown', that enables for the downstream models to calculate the CAC by channel.
#### Data Lineage and Quality Checks:
![stg_events.png](picture/stg_events.png)

**source table**: events
**partition_key**: event_date

For quality check, I used the `dbt test` command to check the data quality, including the event_id is uniqe and not null, user_id is not null. I also use the dbt_utils.equality to compare the data with the source table.

**columns**:
|column|type|description|
|---|---|---|
|event_id|string|The unique identifier of the event.|
|---|---|---|
|user_id|string|The unique identifier of the user.|
|---|---|---|
|refers_to_event_id|string|The unique identifier of the event that this event refers to.|
|---|---|---|
|acquisition_channel|string|The acquisition channel of the user.|
|---|---|---|
|event_type|string|The type of the event.|
|---|---|---|
|page|string|The page of the event.|
|---|---|---|
|schema_version|string|The schema version of the event.|
|---|---|---|
|amount|double|The amount of the event.|
|---|---|---|
|currency|string|The currency of the event.|
|---|---|---|
|tax|double|The tax of the event.|
|---|---|---|
|o_ts|timestamp|The timestamp of the event.|
|---|---|---|
|event_date|date|The date of the event.|
|---|---|---|

Backfilling:
using command 
```
dbt run --select stg_events --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### stg_subscripitions:
This table is the cleaned version of the raw subscriptions data, with some basic data quality checks and transformations. I removed the null values in subscription_id and user_id, added the created_date column.
**source table**: subscriptions
**partition_key**: created_date

columns:
|column|type|description|
|---|---|---|
|subscription_id|string|The unique identifier of the subscription.| 
|---|---|
|user_id|string|The unique identifier of the user.| 
|---|---|
|plan_id|string|The unique identifier of the plan.| 
|---|---|
|price|double|The price of the subscription.| 
|---|---|
|currency|string|The currency of the subscription.| 
|---|---|
|start_date|date|The start date of the subscription.| 
|---|---|
|created_at|timestamp|The timestamp of the subscription.| 
|---|---|
|created_date|date|The date of the subscription.| 
|---|---|
|end_date|date|The end date of the subscription.| 
|---|---|

Backfilling:
using command 
```
dbt run --select stg_subscriptions --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### stg_marketing_spend:
This is a cleaned version of the raw marketing spend data, with some basic data quality checks and transformations. I removed the null values in spend_date and channel, added the spend_date column.
**source table**: marketing_spend
**partition_key**: spend_date

columns:
|column|type|description|
|---|---|---|
|spend_date|date|The date of the spend.|
|---|---|---|
|channel|string|The channel of the spend.|
|---|---|---|
|spend|double|The amount of the spend.|
|---|---|---|

Backfilling:
using command 
```
dbt run --select stg_marketing_spend --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### int_amounts_to_euros:
This table is a helper table to convert the amounts in different currencies to euros. It is used to calculated the daily gross revenue and net revenue.
#### Data Lineage:
![int_amounts_to_euros.png](picture/int_amounts_to_euros.png)

**source tables**: stg_events, stg_currency_rates
**partition_key**: event_date

columns:
|column|type|description|
|---|---|---|
|event_id|string|The unique identifier of the event.|
|---|---|
|user_id|string|The unique identifier of the user.|
|---|---|
|amount|double|The amount of the event.|
|---|---|
|currency|string|The currency of the event.|
|---|---|
|event_type|string|The type of the event.|
|---|---|
|event_date|date|The date of the event.|
|---|---|
|rate|double|The exchange rate of the currency.|
|---|---|
|amount_in_euros|double|The amount of the event in euros.|

Backfilling:
using command 
```
dbt run --select int_amounts_to_euros --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### int_users_first_event
This is a helper table for daily_active_users, which stores the first event date for each user.

#### Data Lineage and Quality Checks:
![int_users_first_event.png](picture/int_users_first_event.png)

**source_table**: stg_events

**columns**:
|column|type|description|
|---|---|---|
|user_id|string|The unique identifier of the user.|
|---|---|
|first_event_date|date|The date of the first event.|

Backfilling:
using command 
```
dbt run --select int_users_first_event --vars '{"start_time": "2026-01-01", "interval": 40}'
```


## Gold Layer
### daily_active_users
This table is a daily active users table, one row per user per day. 
**source_table**: stg_events, int_users_first_event
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|user_id|string|The unique identifier of the user.| 
|---|---|
|event_date|date|The date of the event.| 
|---|---|
|channel|string|The channel of the event.| 
|---|---|
|is_new_user|int|Whether the user is a new user.| 

Backfilling:
using command 
```
dbt run --select daily_active_users --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### daily_revenue_gross
This table is a daily gross revenue table, one row per day, which is the sum of all purchase amount in euros.
#### Data Lineage and Quality Checks:
![daily_revenue_gross.png](picture/daily_revenue_gross.png)

**source_table**: int_amounts_to_euros
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|event_date|date|The date of the event.| 
|---|---|
|gross_revenue|double|The gross revenue of the event.| 

Backfilling:
using command 
```
dbt run --select daily_revenue_gross --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### daily_revenue_net
This table is a daily net revenue table, one row per day, which is the sum of all purchase amount in euros minus the sum of all refund amount in euros.
#### Data Lineage and Quality Checks:
![daily_revenue_net.png](picture/daily_revenue_net.png)

**source_table**: int_amounts_to_euros, daily_revenue_gross
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|event_date|date|The date of the event.| 
|---|---|
|net_revenue|double|The net revenue of the event.| 

Backfilling:
using command 
```
dbt run --select daily_revenue_net --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### mrr_daily
This table is a daily MRR table, one row per day, which is the sum of all net revenue in euros.
#### Data Lineage and Quality Checks:
![mrr_daily.png](picture/mrr_daily.png)

**source_table**: daily_revenue_net
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|
|event_date|date|The date of the event.| 
|---|---|
|mrr|double|The MRR of the event.| 

Backfilling:
using command 
```
dbt run --select mrr_daily --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### weekly_cohort_retention
This table is a weekly cohort retention table, one row per cohort per week, which is the number of retained users divided by the cohort size.
#### Data Lineage and Quality Checks:
![weekly_cohort_retention.png](picture/weekly_cohort_retention.png)

**source_table**: stg_events, int_users_first_event
**partition_key**: cohort_week

**columns**:
|column|type|description|
|---|---|---|
|cohort_week|date|The date of the cohort.| 
|---|---|
|week_0|double|The number of retained users in week 0.| 
|---|---|
|week_1|double|The number of retained users in week 1.| 
|---|---|
|week_2|double|The number of retained users in week 2.| 
|---|---|
|week_3|double|The number of retained users in week 3.| 
|---|---|
|week_4|double|The number of retained users in week 4.| 

Backfilling:
using command 
```
dbt run --select weekly_cohort_retention --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### cac_by_channel
This table is a daily CAC by channel table, one row per day, which is the sum of all marketing spend in euros divided by the number of new users in that day.
#### Data Lineage and Quality Checks:
![cac_by_channel.png](picture/cac_by_channel.png)

**source_table**: stg_marketing_spend, daily_active_users
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|event_date|date|The date of the event.| 
|---|---|
|cac_by_channel|double|The CAC by channel of the event.| 

Backfilling:
using command 
```
dbt run --select cac_by_channel --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### ltv_per_user
This table is a daily LTV per user table, one row per user per day, which is the sum of all net revenue in euros.
#### Data Lineage and Quality Checks:
![ltv_per_user.png](picture/ltv_per_user.png)

**source_table**: stg_events, int_users_first_event
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|user_id|string|The unique identifier of the user.| 
|---|---|
|event_date|date|The date of the event.| 
|---|---|
|channel|string|The channel of the event.| 
|---|---|
|is_new_user|int|Whether the user is a new user.| 

Backfilling:
using command 
```
dbt run --select ltv_per_user --vars '{"start_time": "2026-01-01", "interval": 40}'
```

### ltv_cac_ratio
This table is a daily LTV/CAC ratio table, one row per day, which is the sum of all net revenue in euros divided by the sum of all marketing spend in euros.
#### Data Lineage and Quality Checks:
![ltv_cac_ratio.png](picture/ltv_cac_ratio.png)

**source_table**: stg_events, int_users_first_event
**partition_key**: event_date

**columns**:
|column|type|description|
|---|---|---|
|user_id|string|The unique identifier of the user.| 
|---|---|
|event_date|date|The date of the event.| 
|---|---|
|channel|string|The channel of the event.| 
|---|---|
|is_new_user|int|Whether the user is a new user.| 

Backfilling:
using command 
```
dbt run --select ltv_cac_ratio --vars '{"start_time": "2026-01-01", "interval": 40}'
```

1) Architecture & reasoning (required)
Provide a short document (in your README or separate DESIGN.md) describing:

Bronze/Silver/Gold layers (or equivalent) and why
Storage format choices (e.g., Parquet/Iceberg/Delta; or DuckDB/SQLite; or warehouse tables)
Partitioning/clustering strategy
Incremental strategy (how you avoid full refreshes)
Idempotency strategy (how re‑runs and partial failures behave)
How you handle schema evolution + timestamp normalization
How you handle corrupted rows (quarantine strategy)
Backfill strategy (how you rebuild historical data correctly)
2) Implementation (required)
Implement a runnable pipeline that produces gold tables/views:

daily_active_users
daily_revenue_gross
daily_revenue_net
mrr_daily (or mrr_monthly — explain your choice)
weekly_cohort_retention
cac_by_channel (or overall CAC — explain)
ltv_per_user
ltv_cac_ratio
You may use any stack you prefer (examples):

Python + DuckDB + Parquet
Spark + Iceberg/Delta/Hudi
dbt + BigQuery/Snowflake/Postgres
SQL + local warehouse

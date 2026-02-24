InfluxDB 2 Extractor
=============

Description

The component extracts data from InfluxDB v2 using [Flux query language](https://docs.influxdata.com/influxdb/v2/query-data/flux/) with support for incremental and full load modes.


Configuration
=============


## Component Configuration

At the configuration level, you need to provide:

- **URL** - The URL of the InfluxDB instance including port (e.g., `https://example.com:8086`)
- **Token** - Authentication token for InfluxDB access
- **Organization** - InfluxDB organization name

## Configuration Row Parameters

Each configuration row represents a data extraction job with the following parameters:

### Source Configuration
- **Bucket** - InfluxDB bucket to extract data from (can be selected from available buckets)
- **Query** - Flux query for data extraction with placeholders `{bucket}`, `{start}`, `{batch_size}`, `{offset}`
  - Default: `from(bucket: "{bucket}") |> range(start: {start}) |> limit(n: {batch_size}, offset: {offset})`
- **Start Time** - Absolute or relative time value in FluxQL supported formats:
  - Examples: `-1h`, `2020-05-10T21:00:00Z`, `1567029600`, or `last_run` (uses last successful run time, if unavailable, downloads from the beginning)
  - Default: `last_run`
- **Batch Size** - Number of rows to load per batch (default: 10,000)

### Destination Configuration
- **Name Tables by Tag Values** - When enabled, names the resulting tables of the query based on the value of the tag in the first row (the user must ensure consistency in the query)
  - example query: `from(bucket: "{bucket}")|> range(start: {start})|> limit(n: {batch_size}, offset: {offset}) |> filter(fn: (r) => r["category-tag"] == "a") |> pivot(rowKey: ["_time"], columnKey: ["_measurement", "_field"], valueColumn: "_value"))`.
- **Table Name** - Custom name for destination table (only when query returns one table and tag-based naming is disabled)
- **Preserve Insertion Order** - Maintains row order in destination table (default: enabled, disable if encountering OOM errors)
- **Load Type** - Choose between:
  - **Incremental Load** (default) - Upserts data based on primary key
  - **Full Load** - Overwrites destination table on each run

### Additional Options
- **Debug Mode** - Enables detailed logging for troubleshooting, including every executed query, time taken, and number of rows fetched (default: disabled)

### Query Preview
The component provides a **Query Preview** sync action that lets you test your query directly from the configuration UI before running the full extraction. It executes the configured query with an appended `|> limit(n: 10)` to return only the first 10 rows.

## Notes
- If the table name is too long for Keboola Storage, only the first 40 characters are used, followed by a hash of the complete table name.
- Leading underscores in column names are removed.
- Periods in column names are replaced with double underscores.


Output
======

Each configuration row generates one result table for each table produced by the query.

Development
-----------

To customize the local data folder path, replace the `CUSTOM_FOLDER` placeholder with your desired path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, initialize the workspace, and run the component using the following
commands:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/keboola/component-influxdb-2-ex component-influxdb-2-extractor
cd component-influxdb-2-extractor
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and perform lint checks using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For details about deployment and integration with Keboola, refer to the
[deployment section of the developer
documentation](https://developers.keboola.com/extend/component/deployment/).

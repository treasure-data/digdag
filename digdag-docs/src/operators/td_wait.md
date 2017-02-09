# td_wait>: Waits for data arriving at Treasure Data table

**td_wait>** operator runs a query periodically until it returns true. This operator can use more complex query compared to [td_wait_table> operator](../td_wait_table.html).

    _export:
      td:
        apikey: YOUR/API_KEY
        database: www_access

    +wait:
      td_wait>: queries/check_recent_record.sql

    +step1:
      td>: queries/use_records.sql

Example queries:

    select 1 from target_table where TD_TIME_RANGE(time, '${session_time}') limit 1

    select count(*) > 1000 from target_table where TD_TIME_RANGE(time, '${last_session_time}')

## Secrets

* `td.apikey: API_KEY`

  The Treasure Data API key to use when running Treasure Data queries.

## Options

* `td_wait>: FILE.sql`

  Path to a query template file. This file can contain `${...}` syntax to embed variables.

  * Example: `td_wait>: queries/check_recent_record.sql`

* `database: NAME`

  Name of a database.

  * Example: `database: my_db`

* `apikey: APIKEY`

  API key. This must be set as a secret parameter.

  * Example: `apikey: 992314/abcdef0123456789abcdef0123456789`

* `endpoint: ADDRESS`

  API endpoint (default: api.treasuredata.com).

* `use_ssl: BOOLEAN`

  Enable SSL (https) to access to the endpoint (default: true).

* `engine: presto`

  Query engine (`presto` or `hive`).

  * Example: `engine: hive`

  * Example: `engine: presto`

* `priority: 0`

  Set Priority (From `-2` (VERY LOW) to `2` (VERY HIGH) , default: 0 (NORMAL)).

## Output parameters

* `td.last_job_id`

  The job id this task executed.

  * Example: `52036074`


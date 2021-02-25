
# Digdag profiler (experimental)

Digdag user may sometimes want to know how much attempts and tasks delayed in Digdag. Digdag has an experimental profiler for that.

## How to use

Current profiler requires Digdag source code as follows

```
$ git clone https://github.com/treasure-data/digdag.git
$ cd digdag
$ ./gradlew digdag-profiler:installDist
$ digdag-profiler/build/install/digdag-profiler/bin/digdag-profiler -c $HOME/.config/digdag/digdag.properties --from 2020-12-30T00:00:00
{"attempts":2048,"totalTasks":8192,"totalRunTasks":8190,"totalSuccessTasks":8189,"totalErrorTasks":1,"mostDelayedTask":{"subtaskConfig":{},"exportParams":{},"resetStoreParams":[],"storeParams":{},"report":{"inputs":[],"outputs":[]},"error":{},"resumingTaskId":null,"id":369,"attemptId":1024,"upstreams":[],"updatedAt":"2021-02-16T08:56:01Z","retryAt":null,"startedAt":"2021-02-16T08:56:00Z","stateParams":{},"retryCount":0,"parentId":1000,"fullName":"+echo^failure-alert","config":{"local":{"_type":"notify","_command":"Workflow session attempt failed"},"export":{}},"taskType":0,"state":"success","stateFlags":0},"startDelayMillis":{"min":0,"max":2000,"average":67,"stddev":286},"execDurationMills":{"min":0,"max":11000,"average":1115,"stddev":1546}}
```

## Options

* `-c, --config`

  Configuration file to load. You can use the same configuration file as `digdag server` command uses and only `database.*` properties are necessary in the profiler

  Example: ``-c ~/.config/digdag/config``

* `--from TIMESTAMP`

  The beginning of time range that the profiler scans. The time format is ISO-8601 (`yyyy-MM-dd'T'HH:mm:ss`).

  Example: ``--from 2020-12-30T00:00:00``

* `--to TIMESTAMP`

  The end of time range that the profiler scans. The time format is ISO-8601 (`yyyy-MM-dd'T'HH:mm:ss`). Current time is used if it's not set.

  Example: ``--to 2020-12-31T00:00:00``

* `--fetched-attempts N`

  A parameter for performance tuning. The number of fetched attempt records at once (default: 1000)

  Example: ``--fetched-attempts 2000``

* `--partition-size N`

  A parameter for performance tuning. The number of internal partition size (default: 100)

  Example: ``--partition-size 200``

* `--database-wait-millis N`

  A parameter for performance tuning.  The internal wait (milliseconds) between database transactions (default: 100)

  Example: ``--database-wait-millis 500``

## Outputs

* **attempts**

  The number of attempts

* **totalTasks**

  The number of total tasks

* **totalRunTasks**

  The number of total run tasks

* **totalSuccessTasks**

  The number of total success tasks

* **totalErrorTasks**

  The number of total error tasks

* **mostDelayedTask**

  The task details whose delay of start from the preceding task is the longest

* **startDelayMillis**

  The stats of start delays in milliseconds

* **execDurationMillis**

  The stats of task execution durations in milliseconds


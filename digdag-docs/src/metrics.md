
# Digdag metrics (experimental)

Digdag supports JMX and provides a few metrics. But it is not enough to monitor its performance and stability.
To improve it, new metrics framework _digdag metrics_ has been introduced.
_Digdag metrics_ integrates [micrometer](https://micrometer.io/).
micrometer supports many monitoring tools. Currently _digdag metrics_ supports JMX and Fluentd(Fluency).
We may add a few monitoring tools in the future.

## Metrics category

There are many metrics in _digdag metrics_.
So metrics are categorized as follows.

| Category  | JMX domain        | Metrics name prefix | Description                 |
|-----------|-------------------|---------------------|-----------------------------|
| Agent     | io.digdag.agent   | agent_              | Agent related metrics       |
| API       | io.digdag.api     | api_                | API endpoint metrics        |
| DB        | io.digdag.db      | db_                 | Database access methods     |
| Executor  | io.digdag.executor| executor_           | Workflow executor metrics   |
| Default   | io.digdag         |                     | Others                      |


## Setup
Digdag metrics is disabled as default. To enable it, add configuration to server config as follows.
```
metrics.enable = jmx,fluency
```

As default, all categories metrics are enable.
You can choose enabled categories as follows.
```
metrics.enable = jmx,fluency
metrics.jmx.categories = api,default
metrics.fluency.categories = agent,executor,default
metrics.fluency.host = localhost:24224
metrics.fluency.tag = digdag_metrics
metrics.fluency.step = 60   #step to send metrics. default is 60 seconds
```

## API metrics

API endpoint list and corresponding metrics are as follows

|No.  |Http method| URI                                       | Metrics name                 | Metrics type |
|:---:|-----------|-------------------------------------------|------------------------------|--------------|
|1    | GET       | /api/attempts/{id}                        | api_getAttempt               | timing       |
|2    | GET       | /api/attempts/{id}/retries                | api_getAttemptRetries        | timing       |
|3    | GET       | /api/attempts/{id}/tasks                  | api_getTasks                 | timing       |
|4    | PUT       | /api/attempts                             | api_startAttempt             | timing       |
|5    | POST      | /api/attempts/{id}/kill                   | api_killAttempt              | timing       |
|6    | GET       | /api/logs/{attempt_id}/files              | api_getFileHandles           | timing       |
|7    | GET       | /api/logs/{attempt_id}/files/{file_name}  | api_getFile                  | timing       |
|8    | GET       | /api/project                              | api_getProject               | timing       |
|9    | GET       | /api/projects                             | api_getProjects              | timing       |
|10   | GET       | /api/projects/{id}                        | api_getProjectById           | timing       |
|11   | GET       | /api/projects/{id}/revisions              | api_getRevisions             | timing       |
|12   | GET       | /api/projects/{id}/workflow               | api_getWorkflow              | timing       |
|13   | GET       | /api/projects/{id}/workflows/{name}       | api_getWorkflowByName        | timing       |
|14   | GET       | /api/projects/{id}/workflows              | api_getWorkflows             | timing       |
|15   | GET       | /api/projects/{id}/schedules              | api_getProjectSchedules      | timing       |
|16   | GET       | /api/projects/{id}/sessions               | api_getProjectSessions       | timing       |
|17   | GET       | /api/projects/{id}/archive                | api_getArchive               | timing       |
|18   | DELETE    | /api/projects/{id}                        | api_deleteProject            | timing       |
|19   | PUT       | /api/projects                             | api_putProject               | timing       |
|20   | PUT       | /api/projects/{id}/secrets/{key}          | api_putProjectSecret         | timing       |
|21   | DELETE    | /api/projects/{id}/secrets/{key}          | api_deleteProjectSecret      | timing       |
|22   | GET       | /api/projects/{id}/secrets                | api_getProjectSecretList     | timing       |
|23   | GET       | /api/workflow                             | api_getWorkflowDefinition    | timing       |
|24   | GET       | /api/workflows                            | api_getWorkflowDefinitions   | timing       |
|25   | GET       | /api/workflows/{id}                       | api_getWorkflowDefinitionById| timing       |
|26   | GET       | /api/workflows/{id}/truncated_session_time| api_getTruncatedSessionTime  | timing       |
|27   | GET       | /api/sessions                             | api_getSessions              | timing       |
|28   | GET       | /api/sessions/{id}                        | api_getSession               | timing       |
|29   | GET       | /api/sessions/{id}/attempts               | api_getSessionAttempts       | timing       |
|30   | GET       | /api/schedules                            | api_getSchedules             | timing       |
|31   | GET       | /api/schedules/{id}                       | api_getScheduleById          | timing       |
|32   | POST      | /api/schedules/{id}/skip                  | api_skipSchedule             | timing       |
|33   | POST      | /api/schedules/{id}/backfill              | api_backfillSchedule         | timing       |
|34   | POST      | /api/schedules/{id}/disable               | api_disableSchedule          | timing       |
|35   | POST      | /api/schedules/{id}/enable                | api_enableSchedule           | timing       |


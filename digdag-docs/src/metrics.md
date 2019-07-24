
# Digdag metrics (experimental)

Digdag support JMX and provided a few metrics. But it is not enough to monitor its performance and stability.
To improve it, new metrics framework _digdag metrics_ has been introduced.
These new metrics are provided via JMX.

_Digdag metrics_ integrates micrometer
There are meny metrics in _digdag merics_.
So metrics are categorized as follows.

| Category | JMX domain         | Metrics name prefix | Description                 |
|-----------|-------------------|---------------------|-----------------------------|
| Agent     | io.digdag.agent   | agent_              | Agent related metrics       |
| API       | io.digdag.api     | api_                | API endpoint metrics        |
| DB        | io.digdag.db      | db_                 | Database access methods     |
| Executor  | io.digdag.executor| executor_           | Workflow executor metrics   |
| Default   | io.digdag         |                     | Others                      |


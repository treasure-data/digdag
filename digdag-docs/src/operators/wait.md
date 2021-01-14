# wait>: Wait for a specific duration

**wait>** operator waits a specific duration in the workflow.

This operator is similar to `sh>: sleep 5`, but this is a non-blocking operator and should be always available even in security-restricted environment.

    +wait_10s:
      wait>: 10s

## Options

* **wait>**: DURATION

  Duration to wait.


# wait>: Wait for a specific duration

**wait>** operator waits a specific duration in the workflow.

This operator seems similar to `sh>: sleep 5`, but this works in both blocking and non-blocking modes and should be always available even in security-restricted environment.

    +wait_10s:
      wait>: 10s

## Options

* **wait>**: DURATION

  Duration to wait.

* **blocking**: BOOLEAN

  Digdag agent internally executes this operator in blocking mode and the agent keeps waiting if this option is set to true (default: false)

  Examples:

  ```
  blocking: true
  ```

* **poll_interval**: DURATION

  This option is used only with non-blocking mode. If it's set, digdag agent internally gets awake and checks at a specific interval if the duration has passed. If not set, digdag agent gets awake only when a specific duration passes.

  Examples:

  ```
  poll_interval: 5s
  ```


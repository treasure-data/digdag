# require>: Depends on another workflow

**require>** operator requires completion of another workflow. This operator is similar to [call> operator](call.html), but this operator doesn't start the other workflow if it's already running or has done for the same session time of this workflow. If the workflow is running or newly started, this operator waits until it completes. In  addition, require operator can kick the another project's workflow.

```
# workflow1.dig
+step1:
  require>: another_workflow
```

```
# another_workflow.dig
+step2:
  sh>: tasks/step2.sh
```


## Options

* **require>**: NAME

  Name of a workflow.

  Examples:

  ```
  require>: another_workflow
  ```

* **session_time**: ISO_TIME_WITH_ZONE

  Examples:

  ```
  require>: another_workflow
  session_time: 2017-01-01T00:00:00+00:00
  ```

  ```
  timezone: UTC

  schedule:
    monthly>: 1,09:00:00

  +depend_on_all_daily_workflow_in_month:
    loop>: ${moment(last_session_time).daysInMonth()}
    _do:
      require>: daily_workflow
      session_time: ${moment(last_session_time).add(i, 'day')}
  ```

* **project_id**: project_id

  Id of another project. You can kick another project's workflow by setting this parameter.

  Examples:

  ```
  require>: another_project_wf
  project_id: 12345
  ```

* **ignore_failure**: BOOLEAN

  This operator fails when the dependent workflow finished with errors by default.

  But if `ignore_failure: true` is set, this operator succeeds even when the workflow finished with errors.

  ```
  require>: another_workflow
  ignore_failure: true
  ```


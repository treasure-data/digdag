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
      session_time: ${moment(last_session_time).add(i, 'day').format()}
  ```

* **project_id**: project_id
* **project_name**: project_name

  You can kick another project's workflow by setting project_id or project_name.
  If the project does not exist, the task will fail.
  If you set both project_id and project_name, the task will fail.

  Examples 1:

  ```
  require>: another_project_wf
  project_id: 12345
  ```

  Examples 2:

  ```
  require>: another_project_wf
  project_name: another_project
  ```

* **rerun_on**: none, failed, all (default: none)

  *rerun_on* control require> really kicks or not if the attempt for the dependent workflow already exists. 
  * *none* ... Not kick the workflow if the attempt already exists.
  * *failed* ... Kick the workflow if the attempt exists and its result is not success.
  * *all* ... require> kick the workflow regardless of the result of the attempt.

* **ignore_failure**: BOOLEAN

  This operator fails when the dependent workflow finished with errors by default.

  But if `ignore_failure: true` is set, this operator succeeds even when the workflow finished with errors.

  ```
  require>: another_workflow
  ignore_failure: true
  ```

  require> evaluates *ignore_failure* at last of its process. If rerun_on is set and require> run new attempt, the result of new attempt is checked.

* **params**: MAP

  This operator doesn't pass a parameter to another workflow. `params` options set parameters.

  Examples:

  ```yaml
  +example:
    require>: child
    params:
      param_name1: ${parent_param_name}
  ```

## Notes
- require> has been changed to ignore inherited *retry_attempt_name* parameter. 
  `digdag retry` command generates unique retry_attempt_name to run, but it is not passed to require>.




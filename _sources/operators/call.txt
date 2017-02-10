# call>: Calls another workflow

**call>** operator calls another workflow.

This operator embeds another workflow as a subtask. This operator waits until all tasks of the workflow complete.

```
# workflow1.dig
+step1:
  call>: another_workflow.dig
+step2:
  call>: common/shared_workflow.dig
```

```
# another_workflow.dig
+another:
  sh>: ../scripts/my_script.sh
```

## Options

* **call>**: FILE

  Path to a workflow definition file. File name must end with ``.dig``.
  If called workflow is in a subdirectory, the workflow uses the subdirectory as the working directory. For example, a task has ``call>: common/called_workflow.dig``, using ``queries/data.sql`` file in the called workflow should be ``../queries/data.sql``.

  Examples:

  ```
  call>: another_workflow.dig
  ```

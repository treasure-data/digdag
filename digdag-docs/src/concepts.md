# Concepts

## Projects and revisions

In Digdag, workflows are packaged together with other files used in the workflows. The files can be anything such as SQL scripts, Python/Ruby/Shell scripts, configuration files, etc. This set of the workflow definitions is called project.

When project is uploaded to a Digdag server, Digdag server inserts a new version and keeps old versions. A version of a project is called revision. When you run a workflow, Digdag uses the latest revision by default. But you can also use old revisions for following purposes:

* Check the definition of a past workflow execution.
* Run a workflow using a old revision to reproduce the same results with before.
* Revert to a old revision to fix problems introduced in the latest revision by accident.

A project can contain multiple workflows. But you should create a new project if a new workflow is not related to others. A reason is that all workflows in a project will be updated together when you upload a new revision.


## Sessions and attempts

A session is a plan to run a workflow which should complete successfully. An attempt is an actual execution of a session. A session has multiple attempts if you retry a failed workflow.

The reason why sessions and attempts are separated is that an execution may fail. When you list sessions up, the expected status is that all sessions are green. If you find a failing session, you check attempts of it, and debugs the problem from the logs. You may upload a new revision to fix the issue, then start a new attempt. Sessions let you easily confirm that all planned executions are successfully done.


## Scheduled execution and session_time

A session has a timestamp called `session_time`. This time means "for which time this workflow runs". For example, if a workflow is scheduled every day, the time is usually 00:00:00 of a day such as 2017-01-01 00:00:00. Actual execution time may not be the same time. You may want to delay execution for 2 hours because some data need 1 hour to be prepared. You may run a workflow for the time on a next day to backfill yesterday's results. The time, 2017-01-01 00:00:00 in this example, is called `session_time`.

`session_time` is unique in history of a workflow. If you submit two sessions with the same `session_time`, the later request will be rejected. This prevents accidental submission of a session that ran before for the same time. If you need to run a workflow for the same time, you should retry the past session instead of submitting a new session.


## Tasks

When an attempt of a session starts, a workflow is transformed into a set of tasks. Tasks have dependencies each other. For example, task +dump depends on +process1 and +process2, task +process1 and +process2 depend on +prepare, etc. Digdag understands the dependencies and run the tasks in order.


## Export and store parameters

There are 3 kinds of parameters for a task.

* **local**: parameters directly set to the task
* **export**: parameters exported from parent tasks
* **store**: parameters stored by previous tasks

They are merged into one object when a task runs. Local parameters have the highest priority. Export and store parameters override each other and thus parameters set at later tasks have higher priority.

Export parameters are used for a parent task to pass values to children. Store parameters are used for a task to pass values to all following tasks including children.

Influence of export parameters is limited compared to store parameters. This lets workflows being "modularized". For example, your workflow uses some scripts to process data. You may set some parameters for the scripts to control their behavior. On the other hand, you don't want make the other scripts affected by the parameters (e.g. data loading part shouldn't be affected by any changes in data processing part). In this case, you can put your scripts under a single parent task and let the parent task export parameters.

Store parameters are visible to all following tasks - store parameters are not visible by previous tasks. For example, you ran a workflow and retried it. In this case, parameters stored by a task won't be visible by previous tasks even if the task has finished successfully in the last execution.

Store parameters are not global variables. When two tasks run in parallel, they will use different store parameters. This makes the workflow behavior consitent regardless of actual execution timing. For example, when another task runs depending on the two parallel tasks, parameter stored by the last task will be used in the order of task submission.


## Operators and plugins

Operators are executor of tasks. Operators are set in a workflow definition as `sh>`, `pg>`, etc. When a task runs, Digdag picks one operator, merges all parameters (local, export, and store parameters), then give the merged parameters to the operator.

An operator can be considered as a package of common workload. With operators, you can do the more things with less scripts.

Operators are designed to be plugins (although it's not fully implemented yet). You will install operators to simplify your workflow, and you will create a operator so that other workflows can reuse it. Digdag itself would be a simple platform to run many operators on it.


## Dynamic task generation and _check/_error tasks

Digdag transforms a workflow into a set of tasks with dependencies. This graph of the tasks is called DAG, Directed Acyclic Graph. DAG is good to execute from the most dependent task to the end. However, it can't represent loops. Representing `if` branches is also not straightforward.

But loops and branches are useful. To solve this issue, Digdag dynamically appends tasks to an executing DAG. In following example, Digdag generates 3 tasks to represent a loop: `+example^sub+loop-0`, `+example^sub+loop-1`, and `+example^sub+loop-2` (name of a dynamically generated task starts with `^sub`):

```yaml
+example:
  loop>: 3
  _do:
    echo>: this is ${i}th loop
```

`_check` and `_error` options use dynamic task generation. Those parameters are used by Digdag to run another task only when the task succeeds or fails.

`_check` task is generated after successful completion of a task. This is useful especially when you want to validate results of a task before starting next tasks.

`_error` task is generated after failure of a task. This is useful to notify failure of a task to external systems.

The following example output `success` on succeeding the tasks. And also, It output the message `fail` on failing the tasks.

```yaml
+example:
  sh>: your_script.sh
  _check:
    +succeed:
      echo>: success
  _error:
    +failed:
      echo>: fail
```

## Task naming and resuming

A task has an unique name in an attempt. When you retry an attempt, this name is used to match tasks in the last attempt.

Children tasks have parent task's name as the prefix. Workflow name is also prefixed as the root task. In following example, task names will be `+my_workflow+load+from_mysql+tables`, `+my_workflow+load+from_postgres`, and `+my_workflow+dump`.

```yaml
# my_workflow.dig
+load:
  +from_mysql:
    +tables:
      ...
  +from_postgres:
    ...
+dump:
  ...
```


## Workspace

Workspace is a directory where a task runs at. Digdag extracts files from a project archive to this directory, change directory there, and executes a task (note: local-mode execution does nothing to create a workspace because it's assumed that current working directory is the workspace).

Plugins will not allow access to parent directories of workspace. This is because digdag server is running on a shared environment. A project should be self-contained so that it doesn't have to depend on external environments. Scripting operator is an exception (e.g. sh> operator). It's recommended to run scripts using `docker:` option.


## Next steps

* [Internal architecture](internal.html)
* [Command reference](command_reference.html)


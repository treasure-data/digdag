# Concepts

## Projects and revisions

In Digdag, workflows are packaged together with other files used in the workflows. Files can be anything such as SQL scripts, Python/Ruby/Shell scripts, configuration files, etc. This set of files is called project.

When project is uploaded to a Digdag server, Digdag server inserts a new version and keeps old versions. A version of a project is called revision. When you run a workflow, Digdag uses the latest revision by default. But you can use old revisions for following purposes:

* Check the definition of a past workflow execution.
* Run a workflow using a old revision to reproduce the same results with before.
* Revert to a old revision to fix problems introduced in the latest revision by accident.

A project can contain multiple workflows. You should create a new project if a new workflow is unrelated from others because all workflows in the project will be updated together when you upload a new revision.


## Sessions and attempts

A session is a plan to run a workflow which should complete successfully. An attempt is an actual execution of a session. A session has multiple attempts if you retry a failed workflow.

The reason why sessions and attempts are separated is that an execution may fail. When you list sessions up, the expected status is that all sessions are green. If you find a failing session, you check attempts of it, and debugs the problem from the logs. You may upload a new revision to fix the issue, then start a new attempt. Sessions let you easily confirm that all planned executions are successfully done.


## Scheduled execution and session_time

TODO


## Tasks

TODO


## Resuming

TODO


## Operators

TODO


## Export and store parameters

TODO


## _check and _error tasks


TODO


## Conditional operators and dynamic tasks

TODO


## Task queue and retries

TODO


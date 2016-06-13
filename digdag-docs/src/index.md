
Digdag is a simple tool that helps you to build, run, schedule, and monitor complex pipelines of tasks. It handles dependency resolution so that tasks run in order or in parallel.

Digdag fits simple replacement of cron, IT operations automation, data analytics batch jobs, machine learning pipelines, and many more by using Directed Acyclic Graphs (DAG) as the infrastructure.

# [Getting started](getting_started.html)

[> Starting Digdag in 5 minutes.](getting_started.html)

# Concepts of Digdag

* **Easy to use**

Digdag is a single executable command. Creating new workflow is as easy as writing Makefile. It will come with web-based management UI backed by H2 database or PostgreSQL in the small package.

* **Workflow definition as code**

![Workflow as code](_static/workflow-as-code.png)

Digdag workflow is defined as code. This brings best practice of software development to your pipeline. Defined workflow is dynamically programmable but still reproducible at any time.

* **Grouping tasks**

![Grouping tasks](_static/grouping-tasks.png)

Workflow definition gets complicated quickly. Digdag lets you organize tasks by creating nested groups so that you can see from a bird's view, then dive in to details. Well-organized pipelines make it easy to review your achievement as well as preventing mistakes.


## Table of Contents

```eval_rst
.. toctree::
   :maxdepth: 2

   getting_started.rst
   tutorials.rst
   workflow_definition.rst
   scheduling_workflow.rst
   running_tasks_on_docker.rst
   operators.rst
   reciipes.rst
   command_reference.rst
   python_api.rst
   ruby_api.rst
   releases.rst
```


## License

Digdag is open-source software licensed under [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

See [NOTICE](https://github.com/treasure-data/digdag/blob/master/NOTICE) for details.


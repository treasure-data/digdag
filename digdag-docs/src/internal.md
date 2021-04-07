# Internal architecture

This guide explains implementation details of Digdag. Understanding the internal architecture is helpful for Digdag system administrators and developers to understand why Digdag behaves as described at [architecture](architecture.html) page.

## Task logging

When a task runs, a logger (SLF4j + logback) collects its log messages and passes them to a task logger set at a thread-local storage. When a task starts, task logger creates a new file and writes messages to the file. As the file grows larger than certain limit, it uploads the file to a storage (note: there is an optimization for local execution with local file system logger: logs are directly appended to the final destionation file). LogServer is the SPI interface that implements this storage.

When the task logger uploads a file, it requests a digdag server to send a direct upload URL first. If LogServer supports a temporary pre-signed HTTP URL to upload a file, the server returns a pre-signed URL. Then, the task logger uploads the file to the URL directly. Otherwise, the task logger uploads the file to a server, and the server uploads the contents using LogServer. LogServer stores the file using full name of the task as its path name.

When a client wants log files, the client requests a digdag server to send list of files that have a common path prefix. This prefix may be full name of a task to get logs of a task, or name of a parent task to get all logs of its children. When requested, digdag server gets the list of files from LogServer. LogServer must be capable to list files by prefix.

If LogServer supports a temporary pre-signed HTTP URL to download files, LogServer returns the URL in addition to file name in the list of files. Digdag server returns the list to a client. With this way, downloading traffic won't go throw the server. S3 LogServer supports pre-signed URL, for example. Otherwise, Digdag server returns the list without direct download URL. Clients will request the server to send the files, and the server fetches the contents using LogServer. Default local filesystem LogServer doesn't support pre-signed URL, for example.


## Database

Digdag stores all data in a database (PostgreSQL or H2 database). When digdag runs in local mode, it uses in-memory H2 database.

### Project repository

Tables `projects`, `revisions`, `revision_archives`, `workflow_definitions`, and `workflow_configs` tables store projects.

`projects` table stores names of projects.

`revisions` table stores revisions of projects. It contains information about where project archive is stored. If `archive_type` column is "db", project archive is stored in `revision_archives` table. Otherwise, a project archive is stored using a storage plugin (such as S3).

`workflow_definitions` table stores names of workflows. This table is always used with `workflow_configs` table because actual configurations of workflows are stored there. When a new revision is uploaded, multiple rows are inserted to `workflow_definitions` table because a project can contain multiple workflows. However, in most of cases, only a few workflows are changed. To optimize this workload, a same `workflow_configs` row is shared by multiple `workflow_definitions` if their configurations are identical.


### Schedule store

`schedules` table stores status of active schedules. Actual configuration of schedules are stored in project repository. `schedules` table takes only the latest revision and stores their current status.

When a new revision is uploaded, digdag uses workflow names to update schedules. If a new revision contains a workflow and there is an existent schedule of a old workflow with the same name, digdag keeps status of the schedule. If there're no old workflows with the same name, digdag creates a new schedule. If opposite, an old schedule exists but new revision doesn't include workflows with the same name, the schedule will be deleted.


### Session store

Tables `sessions`, `session_attempts`, `tasks`, `task_details`, `task_dependencies`, and `task_archives` tables store session information.

`sessoins` table stores history of sessions.

`session_attempts` table stores history of attempts.

`tasks`, `task_details`, and `task_dependencies` tables stores tasks of running attempts. `tasks` table stores state of tasks. `task_details` stores config and parameters of tasks, and `task_dependencies` stores dependency between multiple tasks under an parent tasks. Workflow executor checks these tables periodically to run workflows. When an attempt finishes, its tasks will be removed from the tables and archived in `task_archives` table.


## API server, agent, workflow executor, and schedule executor

When Digdag runs as a server, it has 3 major thread pools:

* API server: REST API server. This is the only component that receives requests from external systems.
* Agent: Agent fetches tasks from a queue and runs them. This is planned to be running on untrusted remote environment. Thus agents won't communicate with database directly.
* Workflow executor: Workflow executor checks state of tasks of running attempts on database and pushes ready tasks to a queue.
* Schedule executor: Schedule executor checks state of active schedules on database and starts tasks.

By default, `digdag server` runs all of them. There're some options to disable the components:

* ``--disable-executor-loop`` disables workflow executor and schedule executor.
* ``--disable-scheduler`` disables schedule executor.
* ``--disable-local-agent`` disables agent.

API server, workflow executor, and schedule executor use database (H2 or PostgreSQL) to communicate each other.

Agent and API server use task queue to communicate. And because task queue is built on top of database, they will be a single cluster as long as a database is shared.


## Task queue

Task queue makes sure that a task runs at least once.

When an attempt starts, WorkflowExecutor pushes the root task to a task queue using TaskQueueServer interface. When the task finished, WorkflowExecutor will get a callback when a task finishes at least once. As tasks become ready to execute, WorkflowExecutor pushes the tasks to a task queue.

An agent uses TaskQueueClient interface to fetch tasks. The default implementation of TaskQueueClient is TaskQueueServer itself which fetches tasks from the underlaying storage directly. An intended implementation is HTTP-based client that fetches tasks through Digdag's REST API.

When a task is pushed to a queue, it pushes ID of the task. TaskRequest will be instanciated when an agent fetches it.

When an agent fetches a task, it locks the task first. A locked task won't be taken by other agents for a while. The agent must send heartbeat to extend the lock expiration time until execution of the task finishes. When an agent crashes, heartbeat breaks out. In this case, the task will be taken by another agent. The task is deleted from the queue by WorkflowExecutor when task finish callback is sent.


## Project archive storage

Storage (`io.digdag.spi.Storage`) is a plugin interface that is used to store task log files and project archives. Task log files use storage through StorageFileLogServer (`io.digdag.core.log`) class, and project archives use storage through ArchiveManager (`io.digdag.core.storage`) class.

S3Storage is a storage implementation that stores files on Amazon S3.

Storage is injected using Extension mechanism (see below).


## Command executor

Command executor (`io.digdag.spi.CommandExecutor`) is a plugin interface that is used to execute a command in a sandbox environment.

A sandbox is expected to provide following functionalities to provide multi-user task execution environment:

* Isolated OS image
* Limited CPU consumption
* Limited memory consumption
* Limited disk consumption

An expected use case is that a system administrator of agent configure agent to enforce use of a secure command executor such as Docker, although it is not implemented yet.

Currently, operator code itself (Java code) doesn't run in a sandbox. Operators run in the same environment with agent. Thus operators are required to be secure so that it doesn't leak security information or badly impact on execution environment. If it seems hard to achieve, agent needs another mechanism to isolate execution environment.


## Extension mechanisms

### Extension

Extension (`io.digdag.spi.Extension`) is an interface to statically customize digdag using dependency injection (Guice). This is useful to override some built-in behavior, add built-in operators, or override default parameters.

Extension needs least code to make some extension possible. But it's the hardest to use because users need to write program to use.

A typical use case is for system integrators to customize digdag for their internal use.

Many of customization points in digdag are assuming Extension to override (i.e. most of what's bound using guice) because it needs less code. But for ease of use, they should also accept system plugins, eventually.


### System plugins

System plugins are plugins loaded when digdag starts. System plugins are used to customize global behavior of digdag. Adding an authenticator is one of the use cases.

If `io.digdag.spi.Plugin` implementations are available using Java's ServiceLoader mechanism in classpath, digdag uses them automatically.

If plugins are written in config file (``system-plugin.repositories`` and ``system-plugin.dependencies``), digdag downloads them from remote maven repositories, and loads `io.digdag.spi.Plugin` implementations using ServiceLoader. These plugins are loaded using isolated classloaders.

Plugins share the same Guice instance with digdag core. Thus plugins can get any internal resources as long as an instance is available through `io.digdag.spi` interface which are shared even with isolated classloaders.

A system plugin is instantiated once when digdag starts.

All dynamic plugins can be loaded as system plugins. But system plugins can't be loaded as dynamic plugins.


### Dynamic plugins

Dynamic plugins are plugins loaded when a task runs. Dynamic plugins are loaded using an isolated class loader and isolated Guice instance. Dynamic plugins can access to only subset of internal resources. For example, dynamic plugin loader of operators allows access to CommandExecutor and TemplateEngine.

An intended use case is loading operators.

A dynamic plugin may be instantiated multiple times depending on cache size of DynamicPluginLoader.


## Next steps

* [Command reference](command_reference.html)


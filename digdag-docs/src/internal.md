# Internal architecture

This guide explains implementation details of Digdag. Understanding the internal architecture is helpful for Digdag system administrators and developers to understand why Digdag behaves as described at [architecture](architecture.html) page.

## Task logging

When a task runs, its log messages are collected and stored into files. When a task starts, a new file is created and messages are written to the file. As the file grows larger than certain limit, the file is uploaded to a storage (note: there is an optimization for local execution with local file system logger: logs are directly appended to the final destionation file). There is an SPI interface called LogServer to customize the storage.

When a client uploads a file, the client first requests a direct upload URL. If LogServer supports a temporary pre-signed HTTP URL to upload files, server returns the URL. Then, the client uploads the file to the URL directly. Otherwise, the client uploads the file to a digdag server, and the digdag server passes the contents to LogServer interface.

LogServer stores the file with the full name of the task.

When a client wants log files, the client requests list of files that have a common prefix. This prefix may be full name of a task to get logs of a task, or name of a parent task to get all logs of its children. Digdag server gets the list of files from LogServer. LogServer must be capable to list files by prefix.

If LogServer supports a temporary pre-signed HTTP URL to download files, LogServer returns the URL in addition to file name in the list. Digdag server returns the list to a client. With this way, download traffic won't go throw the server. S3 LogServer supports this, for example.

If LogServer doesn't support pre-signed URL, Digdag server returns file list without direct download URL. Clients request contents of each files to the server, and the server fetches the contents through LogServer API. Default local filesystem LogServer doesn't support pre-signed URL.


## Task queue

Task queue makes sure that a task runs at least once.

When an attempt starts, WorkflowExecutor pushes the root task to a task queue using TaskQueueServer interface. When the task finished, WorkflowExecutor will get a callback when a task finishes at least once. As tasks become ready to execute, WorkflowExecutor pushes the tasks to a task queue.

An agent uses TaskQueueClient interface to fetch tasks. The default implementation of TaskQueueClient is TaskQueueServer itself which fetches tasks from the underlaying storage directly. An intended implementation is HTTP-based client that fetches tasks through Digdag's REST API.

When a task is pushed to a queue, it pushes ID of the task. TaskRequest will be instanciated when an agent fetches it.

When an agent fetches a task, it locks the task first. A locked task won't be taken by other agents for a while. The agent must send heartbeat to extend the lock expiration time until execution of the task finishes. When an agent crashes, heartbeat breaks out. In this case, the task will be taken by another agent. The task is deleted from the queue by WorkflowExecutor when task finish callback is sent.


## Extension mechanisms

### Extension

Extension (`io.digdag.spi.Extension`) is an interface to statically customize Digdag using dependency injection (Guice). This is useful to override some built-in behavior, add built-in operators, or override default parameters.

Extension is the easiest way to let users customize digdag. But it's the hardest to use because users need to write program to use.

A typical use case is for system integrators to customize digdag for their internal use.

Many of customization points in digdag are assuming Extension to override (e.g. `io.digdag.server.Authenticator`) because it needs less code. But for ease of use, eventually they should also accept system plugins.


### System plugins

System plugins are plugins loaded when digdag starts. System plugins are loaded using an isolated class loader but they share the same Guice instance with digdag core. Thus system plugins can get any internal resources as long as an instance is available through `io.digdag.spi` interface.

System plugins are used to customize global behavior of digdag. Adding a scheduler is one of intended use cases (although this is not implemented yet).

A system plugin is instantiated once when digdag starts.

Plugin loader can fetch files from remote maven repositories. Thus, plugins will be released as a jar file with a pom file on a maven repository.

All dynamic plugins can be loaded as system plugins. But system plugins can't be loaded as dynamic plugins.


### Dynamic plugins

Dynamic plugins are plugins loaded when a task runs. Dynamic plugins are loaded using an isolated class loader and isolated Guice instance. Dynamic plugins can access to only allowed resources. For example, dynamic plugin loader of operators allows access to CommandExecutor and TemplateEngine.

An intended use case is loading operators.

A dynamic plugin may be instantiated multiple times depending on cache size of DynamicPluginLoader.

## Next steps

* [Command reference](command_reference.html)


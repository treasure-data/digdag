# digdag

## Quick Start

1. Install Java JDK >= 8
2. Checkout the latest code: `$ git clone https://github.com/treasure-data/digdag.git && cd digdag`
3. Build it: `$ ./gradlew cli`
4. Run example `$ ./pkg/digdag-0.1.0.jar run examples/NAME.yml`
5. (optional) install graphviz: `$ brew install graphviz`

## Commands

```
Usage: digdag <command> [options...]
  Local-mode commands:
    init <path>                      create a new workflow project
    r[un] [+name]                    run a workflow
    c[heck]                          show workflow definitions
    sched[uler]                      run a scheduler server

  Server-mode commands:
    server                           start digdag server

  Client-mode commands:
    archive <workflow.yml...>        create a project archive
    start <repo-name> <+name>        start a new session of a workflow
    kill <session-id>                kill a running session
    workflows [+name]                show registered workflow definitions
    schedules                        show registered schedules
    sessions [repo-name] [+name]     show past and current sessions
    tasks <session-id>               show tasks of a session

  Options:
    -g, --log PATH                   output log messages to a file (default: -)
    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
    -X KEY=VALUE                     add a performance system config
```

Use `<command> --help` to see detailed usage of a command.

## Local mode

With local mode, Digdag runs workflows on the local machine.

### Creating a new project

First, you can use `digdag init` command to create a new project. Example:

```
$ digdag init my-workflow
```

This command creates my-workflow directory with a sample workflow definition file.

### Running a workflow

At above section, `digdag init` creates `my-workflow/digdag` file. You can use this executable file to run a workflow:

```
$ cd my-workflow
$ ./digdag run
```

This `./digdag run` command reads `./digdag.yml` file and runs a workflow defined in the file.

All keys starting with `+` are tasks. Tasks in a workflow run from the top to the bottom one by one. A task can be nested, a nested group has `parallel: true` option, tasks in the group runs in parallel. For more examples, checkout example definitions at [examples/](https://github.com/treasure-data/digdag/blob/master/examples).

### Adding another workflow

You can define multiple workflows in a `digdag.yml` file. For example,

```yaml
run: +main

+main:
  +task1:
    sh>: echo "this main task."

+sub:
  +task1:
    sh>: echo "this a sub task."
```

To run the new workflow named `+sub`, you can use following command:

```
$ ./digdag run +sub
```

### Including files

You can use `{% load ... %}` syntax to organize a large workflow definition. For example,

```yaml
<<: {% load 'workflow1.yml' %}
<<: {% load 'workflow2.yml' %}
<<: {% load 'workflow3.yml' %}
run: +workflow1
```

### Resuming a session

When a workflow runs, digdag saves successful tasks at `./digdag.status` directory. You can use `-s` option to skip previously succeeded tasks:

```
$ digdag run -s digdag.status
```

If you want to retry successful tasks, simply delete files from the directory and run the same command.


### Running sub tasks only

You can run a workflow from the middle.

```
$ digdag run +main+task2
```

This command skips tasks before `+task2`, and runs `+task2` and tasks after `+tasks2`. This is useful when you're developing or debugging a workflow.


### Scheduling a workflow

Above `run` command runs a workflow once. To run workflow periodically, you can use `scheduler` command instead of `run`:

```
$ digdag scheduler
```

A workflow definition needs to include scheduling options:

```yaml
run: +main
  timezone: Europe/Paris
  schedule:
    hourly>: 30:00   # runs at 30-minute every hour
  +task:
    sh>: echo "This schedule is for $session_time"
```

This `digdag scheduler` reloads workflow definition file automatically when it's updated.

You can use client-mode commands (such as `schedules`, `sessions`, and `kill`) to check and manage schedules.


## Server-mode

### 1. Creating a project archive

A project archive is a package that contains workflow definitions, configuration files, scripts, and other data files in a single package. You can register workflows to a server by uploading an archive. The server schedules the workflows, and extracts the archive into a temporary directory when it runs a workflow.

It's recommended to manage files using `git`. You can create a project archive as following:

```
$ digdag init .
$ git init
$ git add digdag*
$ git commit -a -m "the first commit"
$ git ls-files | digdag archive
```

### 2. Starting the server

```
$ digdag server --memory
```

### 3. Uploading the project archive

```
$ curl -v -X PUT -H "Content-Type: application/x-gzip" -T archive.tar.gz -4 "http://localhost:9090/api/repositories?repository=myproject&revision=`git rev-parse HEAD`"
```

### 4. Checking status

```
$ curl -v -4 "http://localhost:9090/api/repositories"
$ curl -v -4 "http://localhost:9090/api/sessions"
```

# Development

## Running test

```
$ ./gradlew check
```

Test coverage report is generated at `didgag-*/build/reports/jacoco/test/html/index.html`.
Findbugs report is generated at `digdag-*/build/reports/findbugs/main.html`.

## Testing with PostgreSQL

Test uses in-memory H2 database by default. To use PostgreSQL, set following environment variables:

```
$ export DIGDAG_TEST_POSTGRESQL="$(cat config/test_postgresql.properties)"
```


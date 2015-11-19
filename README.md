# digdag

## Quick Start

1. Install Java JDK >= 8
2. Checkout the latest code: `$ git checkout https://github.com/treasure-data/digdag.git && cd digdag`
3. Build it: `$ ./gradlew cli`
4. Run example `$ ./pkg/digdag-0.1.0.jar run examples/NAME.yml`
5. (optional) install graphviz: `$ brew install graphviz`

## Commands

```
Usage: digdag <command> [options...]
  Commands:
    run <workflow.yml>               run a workflow
    show <workflow.yml>              visualize a workflow
    sched <workflow.yml> -o <dir>    start scheduling a workflow

  Server-mode commands:
    archive <workflow.yml...>        create a project archive
    server                           start digdag server

  Options:
    -g, --log PATH                   output log messages to a file (default: -)
    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
    -X KEY=VALUE                     add a performance system config
```

Use `<command> --help` to see detailed usage of a command.

## CLI-mode

With Command-Line-Interface, Digdag runs workflows on local machine.

### Running a workflow

`digdag run <workflow.yml>` executes a workflow. A workflow is defined in a YAML file.

All keys starting with `+` are tasks. Tasks run from the top task to the bottom one by one. A task can be nested. If a group of tasks have `parallel: true` option, tasks in the group runs in parallel. For details, checkout example definitions at [examples/](https://github.com/treasure-data/digdag/blob/master/examples).

```
digdag run examples/check_task.yml
```

### Resuming a session

Digdag supports resuming a failed session. When a workflow fails, you can restart it with `-r` option to skip tasks that succeeded before.

```
digdag run config.yml -r config.yml.resume.yml
```

## Server-mode

### 1. Creating a project archive

A project archive is a package that contains workflow definitions, schedule definitions, scripts, and configuration files in a single file.
You can register workflows to a server by uploading an archive. The server schedules the workflows, and extracts the archive into a temporary directory when it runs a workflow.

It's recommended to manage files using `git`. You can create a project archive as following:

```
$ git init
$ git add workflows/*.yml
$ git commit -a -m "first commit"
$ git ls-files | digdag archive -o archive.tar.gz workflows/*.yml
```

* `archive.tar.gz`: output file path.
* `workflows/*.yml`: path to workflow definition files.

### 2. Starting the server

```
$ digdag server
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


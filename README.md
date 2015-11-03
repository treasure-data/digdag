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

  Options:
    -g, --log PATH                   output log messages to a file (default: -)
    -l, --log-level LEVEL            log level (error, warn, info, debug or trace)
    -X KEY=VALUE                     add a performance system config
```

Use `<command> --help` to see detailed usage of a command.


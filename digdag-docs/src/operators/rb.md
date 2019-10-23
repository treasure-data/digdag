# rb>: Ruby scripts

**rb>** operator runs a Ruby script using `ruby` command.

See [Ruby API documents](../ruby_api.html) for details including best practices how to configure the workflow using `_export: require:`.

    _export:
      rb:
        require: tasks/my_workflow

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2

## Options

* **rb>**: [MODULE::CLASS.]METHOD

  Name of a method to run.

  Examples:

  ```
  rb>: Task::MyWorkflow.my_task
  ```

* **require**: FILE

  Name of a file to require.

  Examples:

  ```
  require: task/my_workflow
  ```

* **ruby**: PATH STRING or COMMAND ARGUMENTS LIST

  The ruby defaults to `ruby`. If an alternate ruby and options are desired, use the `ruby` option.

  Examples:

  ```
  ruby: /usr/local/bin/ruby
  ```

  ```
  ruby: ["ruby", "-rbundler/setup"]
  ```

  It is also possible to configure in `_export` section.

  Examples:

  ```
  _export:
    rb:
      ruby: /usr/local/bin/ruby
  ```

## TIPS: Run ruby with bundler

It is possible to run ruby scripts with [bundler](https://bundler.io/) using `BUNDLE_GEMFILE` environment variable and `-rbundler/setup` option as:

    _export:
      BUNDLE_GEMFILE: /path/to/Gemfile
      rb:
        require: tasks/my_workflow
        ruby: ["ruby", "-rbundler/setup"]

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2


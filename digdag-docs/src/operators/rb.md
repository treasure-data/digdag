# rb>: Ruby scripts

**rb>** operator runs a Ruby script using `ruby` command.

See [Ruby API documents](../../ruby_api.html) for details including best practices how to configure the workflow using `_export: require:`.

    _export:
      rb:
        require: tasks/my_workflow

    +step1:
      rb>: my_step1_method
    +step2:
      rb>: Task::MyWorkflow.step2

## Options

* `rb>: [MODULE::CLASS.]METHOD`

  Name of a method to run.

  * Example: `rb>: Task::MyWorkflow.my_task`

* `require: FILE`

  Name of a file to require.

  * Example: `require: task/my_workflow`


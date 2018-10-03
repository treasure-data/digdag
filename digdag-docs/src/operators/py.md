# py>: Python scripts

**py>** operator runs a Python script using `python` command.

See [Python API documents](../../python_api.html) for details including variable mappings to keyword arguments.

    +step1:
      py>: my_step1_method
    +step2:
      py>: tasks.MyWorkflow.step2

## Options

* **py>**: [PACKAGE.CLASS.]METHOD

  Name of a method to run.

  Examples:

  ```
  py>: tasks.MyWorkflow.my_task
  ```

The python defaults to `python`. If an alternate python such as `/opt/conda/bin/python` is desired, use the `python` option in the `_export` section.

    _export:
      py:
        python: ["/opt/conda/bin/python"]

    +step1:
      py>: tasks.MyWorkflow.my_task2

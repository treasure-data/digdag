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

* **python**: PATH STRING or COMMAND ARGUMENTS LIST

  The python defaults to `python`. If an alternate python and options are desired, use the `python` option.

  Examples:

  ```
  python: /opt/conda/bin/python
  ```

  ```
  python: ["python", "-v"]
  ```

  It is also possible to configure in `_export` section.

  Examples:

  ```
  _export:
    py:
      python: /opt/conda/bin/python
  ```

# py>: Python scripts

**py>** operator runs a Python script using `python` command.

See [Python API documents](../python_api.html) for details including variable mappings to keyword arguments.

    +step1:
      py>: my_step1_method
    +step2:
      py>: tasks.MyWorkflow.step2

## Options

* **py>**: [PACKAGE.CLASS.]METHOD

  Name of a method to run.

  Examples:

  ```yaml
  # sample.dig
  py>: tasks.MyWorkflow.my_task
  ```

  This example assume the following directory structure:

  ```
  .
  ├── sample.dig
  └── tasks
      └── __init__.py
  ```

  You can write `__init__.py` like:

  ```python
  # __init__.py
  class MyWorkflow(object):
      def my_task(self):
          print("awesome execution")
  ```

  Or, you can create put a Python script named `tasks.py` in a same directory as dig file.

  ```
  .
  ├── sample.dig
  └── tasks.py
  ```

  Here is the example of `tasks.py`:

  ```python
  # tasks.py
  class MyWorkflow(object):
      def my_task(self):
          print("awesome execution")
  ```

  You can write a function without creating a class as the following:

  ```yaml
  # simple_sample.dig
  py>: simple_tasks.my_func
  ```

  ```
  .
  ├── simple_sample.dig
  └── simple_tasks.py
  ```

  ```python
  # simple_tasks.py
  def my_func():
    print("simple execution")
  ```

* **python**: PATH STRING or COMMAND ARGUMENTS LIST

  The python defaults to `python`. If an alternate python and options are desired, use the `python` option.

  Examples:

  ```yaml
  python: /opt/conda/bin/python
  ```

  ```yaml
  python: ["python", "-v"]
  ```

  It is also possible to configure in `_export` section.

  Examples:

  ```yaml
  _export:
    py:
      python: /opt/conda/bin/python
  ```

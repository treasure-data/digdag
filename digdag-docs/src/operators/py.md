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

  You can pass arguments to class for initialization　by defining arguments under the `py>:` operation as the following:
  ```yaml
  # sample.dig
  +some_task:
    py>: tasks.MyWorkflow.my_task
    required1_1: awesome execution
    required1_2: "awesome execution"
    required2: {a: "a"}
    required3: 1
    required4: 1.0
    required5: [a, 1, 1.0, "a"]
  ```

  Also, you can do the same thing using `_export` as the following:
  ```yaml
  # sample.dig
  +some_task:
    _export:
      required1_1: awesome execution
      required1_2: "awesome execution"
      required2: {a: "a"}
      required3: 1
      required4: 1.0
      required5: [a, 1, 1.0, "a"]
    py>: tasks.MyWorkflow.my_task
  ```

  This example assume following Python script:

  ```python
  # tasks.py
  class MyWorkflow(object):
      def __init__(
        self,
        required1_1: str,
        required1_2: str,
        required2: dict[str, str],
        required3: int,
        required4: float,
        required5: list[Union[str, int, float]]
      ):
          print(f"{required1_1} same as {required1_2}")
          self.arg2 = required2
          print(f"{float(required3)} same as {required4}")
          self.arg5 = required5
      
      def my_task(self):
          pass
  ```

  Or, you can pass arguments to function as the following:

  ```yaml
  # sample.dig
  +some_task:
    py>: simple_tasks.my_func
    required1: simple execution
    required2: {a: "a"}
  ```

  ```yaml
  # simple_sample.dig
  +some_task:
    _export:
      required1: simple execution
      required2: {a: "a"}
    py>: simple_tasks.my_func
  ```

  ```python
  # simple_tasks.py
  def my_func(required1: str, required2: dict[str, str]):
    print(f"{required1}: {required2}")
  ```

  Finally, you can pass combination (must have different names) of class and mehtod arguments to Python script as the following:
  
  ```yaml
  # sample.dig
  +some_task:
    py>: tasks.MyWorkflow.my_task
    required_class_arg: awesome execution
    required_method_arg: ["a", "b"]
  ```

  ```yaml
  # sample.dig
  +some_task:
    _export:
      required_class_arg: awesome execution
      required_method_arg: ["a", "b"]
    py>: tasks.MyWorkflow.my_task
  ```

  ```python
  # tasks.py
  class MyWorkflow:
    def __init__(self, required_class_arg: str):
      self.arg = required_class_arg
    
    def my_task(self, required_method_arg: list[str]):
      print(f"{self.arg}: {required_method_arg}")
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

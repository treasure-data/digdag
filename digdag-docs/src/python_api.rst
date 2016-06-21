Language API - Python
==================================

Programmable workflow in Python
----------------------------------

workflow.dig:

.. code-block:: yaml

    +step1:
      py>: tasks.MyWorkflow.step1

    +step2:
      py>: tasks.MyWorkflow.step2

tasks/__init__.py:

.. code-block:: python

    class MyWorkflow(object):
      def step1(self):
        print("step1")

      def step2(self):
        print("step2")

Defining variables
----------------------------------

.. code-block:: python

    import digdag
    class MyWorkflow(object):
      def step1(self):
        digdag.env.store({'my_value': 1})

      def step2(self):
        print("step2: %s" % digdag.env.params["my_value"])

Method argument mapping
----------------------------------

.. code-block:: python

    import digdag
    class MyWorkflow(object):
      def step1(self, session_time):
        digdag.env.store({'my_value': 1})

      def step2(self, my_value="default"):
        print("step2: %s" % my_value)

Generating child tasks
----------------------------------

Generating Python child tasks:

.. code-block:: python

    import digdag
    class MyWorkflow(object):
      def step1(self):
        digdag.env.add_subtask(MyWorkflow.step3, arg1=1)

      def step2(self, my_value="default"):
        print("step2: %s" % my_value)

      def step3(self, arg1):
        print("step3: %s" % arg1)

Generating other child tasks:

.. code-block:: python

    import digdag
    class MyWorkflow(object):
      def step1(self):
        digdag.env.add_subtask({
          '_type': 'mail',
          'body': 'this is email body in string',
          'subject': 'workflow started',
          'to': ['me@example.com'],
        })

.. note::

    Arguments need to be serializable using JSON. If arguments include non-serializable objects such as function, add_subtask throws TypeError.


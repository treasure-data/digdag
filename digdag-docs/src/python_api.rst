Language API - Python
==================================

Programmable workflow in Python
----------------------------------

digdag.yml:

.. code-block:: yaml

    main: +main

    +main:
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

Exporting variables
----------------------------------

.. code-block:: python

    class MyWorkflow(object):
      def step1(self):
        digdag.env.export({'my_value': 1})

      def step2(self):
        print("step2: %s" % digdag.env.config["my_value"])

Method argument mapping
----------------------------------

.. code-block:: python

    class MyWorkflow(object):
      def step1(self, session_time):
        digdag.env.export({'my_value': 1})

      def step2(self, my_value="default"):
        print("step2: %s" % my_value)

Generating child tasks
----------------------------------

.. code-block:: python

    class MyWorkflow(object):
      def step1(self):
        digdag.env.add_subtask(MyWorkflow.step3, arg1=1)

      def step2(self, my_value="default"):
        print("step2: %s" % my_value)

      def step3(self, arg1):
        print("step3: %s" % arg1)


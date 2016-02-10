Composing workflow
==================================

digdag.yml
----------------------------------

Workflow is defined in a YAML file named "digdag.yml". An example is like this:

.. code-block:: yaml

    run: +main
    
    +main:
      +step1:
        sh>: tasks/shell_sample.sh
    
      +step2:
        py>: tasks.MyWorkflow.step2
        param1: this is param1
    
      +step3:
        rb>: MyWorkflow.step3
        require: tasks/ruby_sample.rb
    
      +step4:
        require>: +another_workflow
    
    +another_workflow:
      +step1:
        sh>: tasks/shell_sample.sh


``run:`` parameter is used to declare the default workflow to run. ``$ digdag run`` command runs this workflow. You can run another workflow using ``$ digdag run +another_workflow`` command.


"+" is a task
----------------------------------

Key names starting with ``+`` sign is a task. Tasks run from the top to bottom in order. A task can be nested as a child of another task. In above example, ``+step2`` runs after ``+step1`` as a child of ``+main`` task.

task types>
----------------------------------

A task with ``type>: command`` parameter executes an action. You can choose various kinds of actions such as running `shell scripts <task_types.html#sh-shell-scripts>`_, `Python methods <task_types.html#py-python-scripts>`_, `sending email <task_types.html#mail-sending-email>`_, etc. See `Task types <task_types.html>`_ page for the list of built-in types.

.. note::

    Setting ``foo>: bar`` parameter is equivalent to setting ``type: foo`` and ``command: bar`` parameters. That is a syntax sugar of setting 2 parameters in 1 line.


Using ${variables}
----------------------------------

A workflow can embed variables using ``${...}`` syntax. You can use built-in variables or define your own variables.

Here is the list of built-in variables:

============================= =========================================== ==========================
Name                          Description                                 Example
============================= =========================================== ==========================
**timezone**                  Timezone of this workflow                   America/Los_Angeles
**session_time**              Time of this session with timezone          2016-01-31T12:45:56-08:00
**session_date**              Date part of session_time                   2016-01-31
**session_date_compact**      Date part of session_time (compact)         20160131
**session_datetime**          Local time format of session_time           2016-01-31 12:45:56
**session_datetime_compact**  Local time format of session_time (compact) 20160131124556
**session_tz_offset**         Timezone offset part of session_time        -0800
**unixtime**                  Seconds since the epoch time                1454273156
============================= =========================================== ==========================

Defining variables
----------------------------------

You can define variables in 3 ways:

* Using ``export`` parameter in YAML
* Setting variable programmably using API
* Starting a session with variables

Using export: parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In YAML file, ``export:`` directive defines variables and following tasks of it can use the variables. With following example, ``+step1`` can use ``foo=1``, and ``+step3`` can use ``foo=1`` and ``bar=2``.

.. code-block:: yaml

    +workflow1:
      export:
        foo: 1

      +step1:
        py>: tasks.MyWorkflow.step1

      +step2:
        py>: tasks.MyWorkflow.step2
        export:
          bar: 2

      +step3:
        py>: tasks.MyWorkflow.step3

Using API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set variables programmably using language API. For exampe, Python API provides ``digdag.task.export_params``:

.. code-block:: python

    import digdag

    class MyWorkflow(object):
      def step2(self):
        digdag.task.export_params["my_param"] = 2

      def step3(self, my_var):
        print("my_var should be 2: %d" % my_var)

See language API documents for details:

* `Python API <python_api.html>`_
* `Ruby API <ruby_api.html>`_
* `Shell script API <shell_api.html>`_

Starting a session with variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set variables when you start a new workflow session. To set variables, use ``-p KEY=VALUE`` multiple times:

.. code-block:: console

    $ digdag run -p my_var1=1 -p my_var2=foo

!include another file
----------------------------------

You can divide a YAML file into small files to organize complex workflow. ``!include`` directive is used to gather those files:

.. code-block:: yaml

    run: +main
    !include : 'main.yml'
    !include : 'another.yml'
    !include : 'theother.yml'

Parallel execution
----------------------------------

If ``parallel: true`` parameter is set, child tasks run in parallel:

.. code-block:: yaml

    run: +main

    +main:
      +prepare
        # +data1, +data2, and +data3 run in parallel.
        parallel: true

        +data1:
          sh>: tasks/prepare_data1.sh

        +data2:
          sh>: tasks/prepare_data2.sh

        +data3:
          sh>: tasks/prepare_data3.sh

      +analyze
          sh>: tasks/analyze_prepared_data_sets.sh


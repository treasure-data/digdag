Composing workflow
==================================

digdag.yml
----------------------------------

Workflow is defined in a YAML file named "digdag.yml".

An example digdag.yml file is like this:

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

Key names starting with ``+`` sign is a task. Tasks run from the beggning to the end in order.

A task can be nested as a child of another task. In above example, ``+step2`` runs after ``+step1`` as a child of ``+main`` task.

task types>
----------------------------------

A task with ``type>: command`` parameter runs an action. You can choose various kinds of actions such as running `shell scripts <task_types.html#sh-shell-scripts>`_, `Python methods <task_types.html#py-python-scripts>`_, `sending email <task_types.html#mail-sending-email>`_, etc. See `Task types <task_types.html>`_ page for the list of available types and examples.

.. note::

    Setting ``foo>: bar`` parameter is equivalent to setting ``type: foo`` and ``command: bar`` parameters. That is a syntax sugar of setting 2 parameters in 1 line.


Using ${variables}
----------------------------------

A workflow can embed variables using ``${...}`` syntax. You can use built-in variables or define your own variables in the workflow.

Here is the list of built-in variables:

====================  ============================================ ==========================
Name                  Description                                  Example
====================  ============================================ ==========================
**${session_name}**   Unique time of the session                   2016-02-09T03:19:21.597Z
**${timezone}**       Configured timezone of the workflow          America/Los_Angeles
====================  ============================================ ==========================

Defining variables
----------------------------------

You can define variables using 3 ways:

* Using ``export`` parameter in YAML
* Setting variable programmably using API
* Starting a session with variables

Using export parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In YAML file, ``export:`` directive defines a variable. Following tasks can use the defined variables. With following example, ``+step3`` can see ``my_var=1``.

.. code-block:: yaml

    +workflow1:
      +step1:
        py>: tasks.MyWorkflow.step1

      +step2:
        py>: tasks.MyWorkflow.step2
        export:
          my_var: 1

      +step3:
        py>: tasks.MyWorkflow.step3

Using API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With API, you can set variables programmably. For exampe, Python API is ``digdag.task.export_params``:

.. code-block:: python

    import digdag

    class MyWorkflow(object):
      def step2(self):
        digdag.task.export_params["my_param"] = 2

      def step3(self, my_var):
        print("my_var should be 2: %d" % my_var)

Starting a session with variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set variables when you start a new workflow session. To set variables, use ``-p KEY=VALUE`` multiple times:

.. code-block:: console

    $ digdag run -p my_var1=1 -p foo=bar

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
      parallel: true

      # +step1, +step2, and +step3 run in parallel

      +step1:
        sh>: tasks/step1.sh

      +step2:
        sh>: tasks/step2.sh

      +step3:
        sh>: tasks/step3.sh


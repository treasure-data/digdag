Workflow definition
==================================

.. contents::
   :local:

Workflow definition: \*.dig files
----------------------------------

A digdag workflow is defined in a file named with ``.dig`` suffix. Name of the file is name of the workflow.

For example, if you create ``hello_world`` workflow, you will create ``hello_world.dig`` file. Contents of the file looks like this:

.. code-block:: yaml

    timezone: UTC

    +step1:
      sh>: tasks/shell_sample.sh

    +step2:
      py>: tasks.MyWorkflow.step2
      param1: this is param1

    +step3:
      rb>: MyWorkflow.step3
      require: tasks/ruby_sample.rb

The ``timezone`` parameter is used to configure the time zone of the workflow and affects session timestamp variables and scheduling. The default time zone is ``UTC``. Some examples of other valid time zones are ``America/Los_Angeles``, ``Europe/Berlin``, ``Asia/Tokyo``, etc.


"+" is a task
----------------------------------

Key names starting with ``+`` sign is a task. Tasks run from the top to bottom in order. A task can be nested as a child of another task. In above example, ``+step2`` runs after ``+step1`` as a child of ``+main`` task.

operators>
----------------------------------

A task with ``type>: command`` or ``_type: NAME`` parameter executes an action. You can choose various kinds of operators such as `running shell scripts <operators/sh.html>`_, `Python methods <operators/py.html>`_, `sending email <operators/mail.html>`_, etc. See `Operators <operators.html>`_ page for the list of built-in operators.

.. note::

    Setting ``foo>: bar`` parameter is equivalent to setting ``_type: foo`` and ``_command: bar`` parameters. That is a syntax sugar of setting 2 parameters in 1 line.


Using ${variables}
----------------------------------

Workflow can embed variables using ``${...}`` syntax. You can use built-in variables or define your own variables.

Here is the list of built-in variables:

=============================== =========================================== ==========================
Name                            Description                                 Example
=============================== =========================================== ==========================
**timezone**                    Timezone of this workflow                   America/Los_Angeles
**project_id**                  The project ID of this workflow             12345
**session_uuid**                Unique UUID of this session                 414a8b9e-b365-4394-916a-f0ed9987bd2b
**session_id**                  Integer ID of this session                  2381
**session_time**                Time of this session with time zone         2016-01-30T00:00:00-08:00
**session_date**                Date part of session_time                   2016-01-30
**session_date_compact**        Date part of session_time (compact)         20160130
**session_local_time**          Local time format of session_time           2016-01-30 00:00:00
**session_tz_offset**           Time zone offset part of session_time       -0800
**session_unixtime**            Seconds since the epoch time                1454140800
**task_name**                   Name of this task                           +my_workflow+parent_task+child_task0
**attempt_id**                  Integer ID of this attempt                  7
=============================== =========================================== ==========================

If `schedule: option is set <scheduling_workflow.html>`_, **last_session_time** and **next_session_time** are also available as following:

==================================== ========================== ==========================
Name                                 Example (hourly schedule)  Example (daily schedule)
==================================== ========================== ==========================
**last_session_time**                2016-01-29T23:00:00-08:00  2016-01-29T00:00:00-08:00
**last_session_date**                2016-01-29                 2016-01-29
**last_session_date_compact**        20160129                   20160129
**last_session_local_time**          2016-01-29 23:00:00        2016-01-29 00:00:00
**last_session_tz_offset**           -0800                      -0800
**last_session_unixtime**            1454137200                 1454054400
**last_executed_session_time**       2016-01-29T23:00:00-08:00  2016-01-29T00:00:00-08:00
**last_executed_session_unixtime**   1454137200                 1454054400
**next_session_time**                2016-01-30T01:00:00-08:00  2016-01-31T00:00:00-08:00
**next_session_date**                2016-01-30                 2016-01-31
**next_session_date_compact**        20160130                   20160131
**next_session_local_time**          2016-01-30 01:00:00        2016-01-31 00:00:00
**next_session_tz_offset**           -0800                      -0800
**next_session_unixtime**            1454144400                 1454227200
==================================== ========================== ==========================

last_session_time is the timestamp of the last schedule. If the schedule is hourly, it's the last hour. If the schedule is daily, it's yesterday. It doesn't matter whether the last schedule actually ran or not. It's simply set to the last timestamp calculated from the current session time.
last_executed_session_time variable is the previously executed session time.

Calculating variables
----------------------------------

You can use basic JavaScript scripts in ``${...}`` syntax to calculate variables.

A common use case is formatting timestamp in different format. Digdag bundles `Moment.js <http://momentjs.com/>`_ for time calculation.

.. code-block:: yaml

  timezone: America/Los_Angeles

  +format_session_time:
    # "2016-09-24 00:00:00 -0700"
    echo>: ${moment(session_time).format("YYYY-MM-DD HH:mm:ss Z")}

  +format_in_utc:
    # "2016-09-24 07:00:00"
    echo>: ${moment(session_time).utc().format("YYYY-MM-DD HH:mm:ss")}

  +format_tomorrow:
    # "September 24, 2016 12:00 AM"
    echo>: ${moment(session_time).add(1, 'days').format("LLL")}

  +get_execution_time:
    # "2016-09-24 05:24:49 -0700"
    echo>: ${moment().format("YYYY-MM-DD HH:mm:ss Z")}

Defining variables
----------------------------------

You can define variables in 3 ways:

* Using ``_export`` parameter in YAML
* Setting variable programmably using API
* Starting a session with variables

.. note::

    You cannot overwrite built-in variables.


Using _export: parameter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In a YAML file, ``_export:`` directive defines variables. This is useful to load static configurations such as host name of a database.

If a task has ``_export`` directive, the task and its children can use the variables because it defines variables in a scope. With following example, all tasks can use ``foo=1`` but only +step1 (and +analyze) can use ``bar=2``.

.. code-block:: yaml

    _export:
      foo: 1

    +prepare:
      py>: tasks.MyWorkflow.prepare

    +analyze:
      _export:
        bar: 2

      +step1:
        py>: tasks.MyWorkflow.analyze_step1

    +dump:
      py>: tasks.MyWorkflow.dump

Using API
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set variables programmably using language API. For example, Python API provides ``digdag.env.export`` and ``digdag.env.store``:

.. code-block:: python

    import digdag

    class MyWorkflow(object):
      def prepare(self):
        digdag.env.store({"my_param": 2})

      def analyze(self, my_var):
        print("my_var should be 2: %d" % my_var)

``digdag.env.store(dict)`` stores variables so that all following tasks (including tasks which are not children of the task) can use them.

.. code-block:: python

    import digdag

    class MyWorkflow(object):
      def export_and_call_child(self):
        digdag.env.export({"my_param": 2})
        digdag.env.add_subtask({'_type': 'call', '_command': 'child1.dig'})

``digdag.env.export(dict)`` is same with "_export" directive in YAML file. It defines variables for their children.

See language API documents for details:

* `Python API <python_api.html>`_
* `Ruby API <ruby_api.html>`_

Starting a session with variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can set variables when you start a new workflow session. To set variables, use ``-p KEY=VALUE`` multiple times:

.. code-block:: console

    $ digdag run -p my_var1=foo -p my_var2=abc

!include another file
----------------------------------

You can divide a YAML file into small files to organize complex workflow. ``!include`` directive is used to gather those files:

.. code-block:: yaml

    _export:
      mysql:
        !include : 'config/mysql.dig'
      hive:
        !include : 'config/hive.dig'

    !include : 'tasks/foo.dig'

.. note::

    A whitespace character before ``:`` is necessary by a limitation to be a valid YAML.


Parallel execution
----------------------------------

If ``_parallel: true`` parameter is set to a group, child tasks in the group run in parallel (grandchildren are not affected):

.. code-block:: yaml

    +prepare:
      # +data1, +data2, and +data3 run in parallel.
      _parallel: true

      +data1:
        sh>: tasks/prepare_data1.sh

      +data2:
        sh>: tasks/prepare_data2.sh

      +data3:
        sh>: tasks/prepare_data3.sh

    +analyze:
      sh>: tasks/analyze_prepared_data_sets.sh

If ``_parallel: {limit: N}`` (N is an integer: 1, 2, 3, …) parameter is set to a group,
child tasks in the group run in parallel is limited to N  (grandchildren are not affected):

.. code-block:: yaml

    +prepare:
      # +data1 and +data2 run in parallel, then +data3 and +data4 run in parallel.
      # (+data1 and +data2 need to be successful.)
      _parallel:
        limit: 2

      +data1:
        sh>: tasks/prepare_data1.sh

      +data2:
        sh>: tasks/prepare_data2.sh

      +data3:
        sh>: tasks/prepare_data3.sh

      +data4:
        sh>: tasks/prepare_data4.sh

    +analyze:
      sh>: tasks/analyze_prepared_data_sets.sh

If ``_background: true`` parameter is set to a task or group, the task or group run in parallel with previous tasks. Next task wait for the completion of the background task or group.

.. code-block:: yaml

    +prepare:
      +data1:
        sh>: tasks/prepare_data1.sh

      # +data1 and +data2 run in parallel.
      +data2:
        _background: true
        sh>: tasks/prepare_data2.sh

      # +data3 runs after +data1 and +data2.
      +data3:
        sh>: tasks/prepare_data3.sh

    +analyze:
      sh>: tasks/analyze_prepared_data_sets.sh

Retrying failed tasks automatically
-----------------------------------

If ``_retry: N`` (N is an integer: 1, 2, 3, ...) parameter is set to a group, it retries the group from the beginning when one or more children failed.

.. code-block:: yaml

    +prepare:
      # If +erase_table, +load_data, or +check_loaded_data fail, it retries from
      # +erase_table again.
      _retry: 3

      +erase_table:
        sh>: tasks/erase_table.sh

      +load_data:
        sh>: tasks/load_data.sh

      +check_loaded_data:
        sh>: tasks/check_loaded_data.sh

    +analyze:
      sh>: tasks/analyze_prepared_data_sets.sh


Tasks also support ``_retry: N`` parameter to retry the specific task. Note that some operators don't support the generic ``_retry`` option but have their own options to control retrying behavior. Operators that involve external systems may reuse previous results. To ensure an external task is repeated, you may use ``_retry`` at the group level.

You can set interval to _retry as follows.

.. code-block:: yaml

    +prepare:
      _retry:
        limit: 3
        interval: 10
        interval_type: exponential



``limit`` is number of retry.
``interval`` is interval time (seconds).
Additionaly you can choose ``interval_type`` as ``constant`` or ``exponential``.
If you set ``constant`` (default) , interval time is constant as set by ``limit``.
If you set ``exponential``, interval time increases with each retry as ``interval x 2^(retry_count-1)``.
In the above example, first retry interval is 10 secs, second is 20 secs, third is 40 secs.


Sending error notification
----------------------------------

If an operator configuration is set at ``_error:`` parameter, the operator runs when the workflow fails.

.. code-block:: yaml

    # this task runs when a workflow fails.
    _error:
      sh>: tasks/runs_when_workflow_failed.sh

    +analyze:
      sh>: tasks/analyze_prepared_data_sets.sh

To send mails, you can use `mail> operator <operators/mail.html>`_.


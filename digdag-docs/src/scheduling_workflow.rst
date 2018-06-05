Scheduling workflow
==================================

.. contents::
   :local:

Setting up a schedule:
----------------------------------

To run a workflow periodically, add a ``schedule:`` option to top-level workflow definitions.

.. code-block:: yaml

    timezone: UTC

    schedule:
      daily>: 07:00:00

    +step1:
      sh>: tasks/shell_sample.sh

In ``schedule:`` directive, you can choose one of following options:

=============================== =========================================== ==========================
Syntax                          Description                                 Example
=============================== =========================================== ==========================
daily>: ``HH:MM:SS``            Run this job every day at HH:MM:SS          daily>: 07:00:00
hourly>: ``MM:SS``              Run this job every hour at MM:SS            hourly>: 30:00
weekly>: ``DDD,HH:MM:SS``       Run this job every week on DDD at HH:MM:SS  weekly>: Sun,09:00:00
monthly>: ``D,HH:MM:SS``        Run this job every month on D at HH:MM:SS   monthly>: 1,09:00:00
minutes_interval>: ``M``        Run this job every this number of minutes   minutes_interval>: 30
cron>: ``CRON``                 Use cron format for complex scheduling      cron>: 42 4 1 * *
=============================== =========================================== ==========================

``digdag check`` command shows when the first schedule will start:

.. code-block:: console

    $ ./digdag check
      ...
    
      Schedules (1 entries):
        daily_job:
          daily>: "07:00:00"
          first session time: 2016-02-10 16:00:00 -0800
          first runs at: 2016-02-10 23:00:00 -0800 (11h 16m 32s later)

.. note::

    When a field is starting with ``*`` , enclosing in quotes is neccessary by a limitasion to be a vaild YAML.


Running scheduler
----------------------------------

``digdag scheduler`` command runs the schedules:

.. code-block:: console

    $ ./digdag scheduler

When you change workflow definition, the scheduler reloads ``digdag.dig`` file automatically so that you don't have to restart it.

Checking scheduling status
----------------------------------

You can use `Client-mode commands <command_reference.html#client-mode-commands>`_ to manage the schedules.

.. note::

    The scheduler command listens on ``http://127.0.0.1:65432`` by default. It accepts connection only from 127.0.0.1 (localhost). This is for a security reason so that it doesn't open the port to the public network. To change the listen address, please use ``--bind ADDRESS`` option.

Setting an alert if a workflow doesn't finish within expected time
--------------------------------------------------------------------

.. code-block:: yaml

    timezone: UTC

    schedule:
      daily>: 07:00:00

    sla:
      # triggers this task at 02:00
      time: 02:00
      +notice:
        sh>: notice.sh

    +long_running_job:
      sh>: long_running_job.sh


Skipping a next workflow session
----------------------------------

Sometimes you have frequently running workflows (e.g. sessions every 30 or 60 minutes) that take longer than the duration between sessions. This variability in the duration of a workflow can occur for a number reasons. For example, you may be seeing an increase in the amount of data you are normally processing.

For example, let’s say we have a workflow that is running hourly, and it normally takes only 30 minutes. But it’s the holiday season and now there has been a huge increase in usage of your site – so much data is now being process the workflow is taking 1 hour and 30 minutes. During this time period, a 2nd workflow has started running for the following hour, which causes further strain on your available resources because both are running at the same time.

It’s this case it’s best to skip the next hour’s workflow session, and instead utilize the subsequent session to process 2 hours of data. To do this, we’ve added the following:

* Added a `skip_on_overtime: true | false` schedule option that can be used to control whether scheduled session execution should be skipped if another session is already running.
* Scheduled workflow sessions now have a `last_executed_session_time` variable which contains the previously executed session time. It is usually same with `last_session_time` but has different value when `skip_on_overtime: true` is set or the session is the first execution.

Skipping backfill.
------------------

When Digdag restart after stopped for a while, it creates past session automatically(called backfill).
By default, It creates past session until the next of `last_session_time`.
The `skip_delayed_by` option skip creating it.

For example, If Digdag restart at 20:00:00 and workflow scheduled as below, Digdag create three sessions(19:59:00, 19:58:00 and 19:57:00).
And Digdag doesn't create sessions which are before 19:56:00.


.. code-block:: yaml

    schedule:
      minutes_interval>: 1
      skip_delayed_by: 3m

    +setup:
      sh>: echo ${session_time}

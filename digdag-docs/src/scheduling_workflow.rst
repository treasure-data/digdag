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
hourly>: ``MM:SS``              Run this job every hour at MM:SS            hourly>: 30:00
daily>: ``HH:MM:SS``            Run this job every day at HH:MM:SS          daily>: 07:00:00
weekly>: ``DDD,HH:MM:SS``       Run this job every week on DDD at HH:MM:SS  weekly>: Sun,09:00:00
monthly>: ``D,HH:MM:SS``        Run this job every month on D at HH:MM:SS   monthly>: 1,09:00:00
minutes_interval>: ``M``        Run this job every this number of minutes   minutes_interval>: 30
cron>: ``CRON``                 Use cron format for complex scheduling      cron>: 42 4 1 * *
=============================== =========================================== ==========================

.. note::

    When a field is starting with ``*`` , enclosing in quotes is necessary by a limitasion to be a vaild YAML.

``digdag check`` command shows when the first schedule will start:

.. code-block:: console

    $ ./digdag check
      ...

      Schedules (1 entries):
        daily_job:
          daily>: "07:00:00"
          first session time: 2016-02-10 16:00:00 -0800
          first scheduled to run at: 2016-02-10 23:00:00 -0800 (in 11h 16m 32s)

.. note::

    | When you use ``hourly``, ``daily``, ``weekly`` or ``monthly``, a session time may not be same with actual run time.
    | The session time is actual run day's 00:00:00 (in case ``hourly``, hour's 00:00).

    .. table:: Schedule Examples (As of system clock: 2019-02-24 14:20:10 +0900)

        ======================= ========================= =========================
        schedule                first session time        first scheduled to run at
        ======================= ========================= =========================
        hourly>: "32:32"        2019-02-24 14:00:00 +0900 2019-02-24 14:32:32 +0900
        daily>: "10:32:32"      2019-02-25 00:00:00 +0900 2019-02-25 10:32:32 +0900
        weekly>: "2,10:32:32"   2019-02-26 00:00:00 +0900 2019-02-26 10:32:32 +0900
        monthly>: "2,10:32:32"  2019-03-02 00:00:00 +0900 2019-03-02 10:32:32 +0900
        ======================= ========================= =========================


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

In sla: directive, you can select either the ``time`` or ``duration`` option.

=============================== ================================================== ==========================
Syntax                          Description                                        Example
=============================== ================================================== ==========================
time: ``HH:MM:SS``             Set this job must be completed by ``HH:MM:SS``     time: 12:30:00
duration: ``HH:MM:SS``         Set this job must be completed during ``HH:MM:SS`` duration: 00:05:00
=============================== ================================================== ==========================

Options
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This parameter supports fail: BOOLEAN and alert: BOOLEAN options. Setting fail: true makes the workflow failed. Setting alert: true sends an notification using above notification mechanism.

* Setting ``fail: true`` makes the workflow failed.
* Setting ``alert: true`` sends an notification using above notification mechanism.


Skipping a next workflow session
----------------------------------

Sometimes you have frequently running workflows (e.g. sessions every 30 or 60 minutes) that take longer than the duration between sessions. This variability in the duration of a workflow can occur for a number reasons. For example, you may be seeing an increase in the amount of data you are normally processing.

For example, let’s say we have a workflow that is running hourly, and it normally takes only 30 minutes. But it’s the holiday season and now there has been a huge increase in usage of your site – so much data is now being process the workflow is taking 1 hour and 30 minutes. During this time period, a 2nd workflow has started running for the following hour, which causes further strain on your available resources because both are running at the same time.

It’s this case it’s best to skip the next hour’s workflow session, and instead utilize the subsequent session to process 2 hours of data. To do this, we’ve added the following:

* Added a ``skip_on_overtime: true | false`` schedule option that can be used to control whether scheduled session execution should be skipped if another session is already running.
* Scheduled workflow sessions now have a ``last_executed_session_time`` variable which contains the previously executed session time. It is usually same with ``last_session_time`` but has different value when ``skip_on_overtime: true`` is set or the session is the first execution.

Skipping backfill.
------------------

The ``skip_delayed_by`` option enables `backfill <command_reference.html#backfill>`_ command to skip creating sessions delayed by the specified time. When Digdag restarts, sessions of a schedule are automatically created until the next of ``last_session_time``.

For example, If Digdag restarts at 20:00:00 and a workflow scheduled as below, it creates three sessions (19:59:00, 19:58:00 and 19:57:00). And then, Digdag doesn't create sessions which are before 19:56:00 by the option.


.. code-block:: yaml

    schedule:
      minutes_interval>: 1
      skip_delayed_by: 3m

    +setup:
      sh>: echo ${session_time}

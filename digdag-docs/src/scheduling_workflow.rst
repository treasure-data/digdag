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


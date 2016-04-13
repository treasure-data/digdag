Getting started
==================================

.. contents::
   :local:
   :depth: 1

1. Downloading the latest version
----------------------------------

`Digdag <index.html>`_ is a simple executable file. You can download the file to ``~/bin`` directory using ``curl`` command as following:

.. code-block:: console

    $ curl -u beta -o ~/bin/digdag --create-dirs -L "https://dl.digdag.io/digdag-latest"
    $ chmod +x ~/bin/digdag

If ``digdag --help`` command works, Digdag is installed successfully.

On Windows, or if curl command doesn't work
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

On Windows, you can download a file named ``digdag-<VERSION>.jar`` from `https://dl.digdag.io/digdag-latest <https://dl.digdag.io/digdag-latest>`_. This downloaded file is an executable bat file (as well as a jar file and a UNIX shell script).

Once download completed, please rename the file to ``digdag.bat`` (Windows) or ``~/bin/digdag`` (Linux/UNIX).

Got error?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you got an error such as **'Unsupported major.minor version 52.0'**, please download and install the latest `Java SE Development Kit 8 <http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>`_ (must be newer than **8u72**).

2. Running sample workflow
----------------------------------

``digdag new <dir>`` command generates sample workflow for you:

.. code-block:: console

    $ digdag new mydag
    $ cd mydag
    $ ./digdag run

Did it work? Next step is adding tasks to ``workflow.yml`` file to automate your jobs.

3. (Adding a data loding task from MySQL to CSV files...)
----------------------------------

4. (Adding a Python task with variables ...)
----------------------------------

5. (Sending an email if a task fails)
----------------------------------

6. (Resuming a failed session from the middle)
----------------------------------

Next steps
----------------------------------

* `Scheduling workflow <scheduling_workflow.html>`_
* `Detailed documents about workflow definition <workflow_definition.html>`_
* `More choices of operators <operators.html>`_


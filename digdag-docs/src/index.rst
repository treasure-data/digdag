
Digdag is a simple tool that helps you to build, run, schedule, and monitor complex pipelines of tasks.

It handles dependency resolution so that tasks run in order or in parallel.

Digdag fits simple replacement of cron, IT operations automation, data analytics batch jobs, machine learning pipelines, and many more by using Directed Acyclic Graphs (DAG) as the infrastructure.

Concepts
==================================

* **Easy to use**

Digdag is a single executable command. Creating new workflow is easy as writing Makefile. It will come with web-based management UI backed by H2 database or PostgreSQL in the small package.

* **Workflow definition as code**

  .. figure:: _static/workflow-as-code.png
     :alt: Grouping tasks
     :align: left

Digdag workflow is defined as code. This brings best practice of software development to your pipeline. Defined workflow is dynamically programmable but still reproducible at any time.

* **Grouping tasks**

  .. figure:: _static/grouping-tasks.png
     :alt: Grouping tasks
     :align: left

Workflow definition gets complicated quickly. Digdag lets you organize tasks by creating nested groups so that you can see from a bird's view, then dive in to details. Well-organized pipelines make it easy to review your achievement as well as preventing mistakes.

Getting started
==================================

1. Downloading the latest version
----------------------------------

Digdag is a simple executable file. You can download the file using ``curl`` command as following:

.. code-block:: console

    $ curl -u beta -o ~/bin/digdag -L "http://localhost:8580/digdag-latest"
    $ chmod +x ~/bin/digdag

If ``digdag --help`` command works, Digdag is installed successfully.

2. Creating sample workflow
----------------------------------

``embulk new <dir>`` command generates sample workflow for you:

.. code-block:: console

    $ digdag new mydag
    $ cd mydag
    $ ./digdag run

Did it work? Next step is `adding tasks <composing_workflow.html>`_ to ``digdag.yml`` file to automate your jobs.

Got error?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If you got an error such as **'Unsupported major.minor version 52.0'**, please download and install the latest `Java SE Development Kit 8 <http://www.oracle.com/technetwork/java/javase/downloads/jdk8-downloads-2133151.html>`_ (must be newer than **8u72**).

Table of Contents
----------------------------------

.. toctree::
   :maxdepth: 2

   composing_workflow.rst
   scheduling_workflow.rst
   task_types.rst
   command_line_interface.rst
   digdag_server.rst
   python_api.rst
   ruby_api.rst


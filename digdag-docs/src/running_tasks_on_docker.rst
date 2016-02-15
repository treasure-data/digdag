Running tasks on Docker
==================================

If ``docker`` option is set, tasks run in a docker container.

.. code-block:: yaml

    run: +main

    export:
      docker:
        image: ubuntu:14.04
    
    +main:
      +step1:
        py>: tasks.MyWorkflow.step1


Running tasks on Docker
==================================

If ``docker`` option is set, tasks run in a docker container.

.. code-block:: yaml

    _export:
      docker:
        image: ubuntu:14.04
    
    +step1:
      py>: tasks.MyWorkflow.step1

Running build commands
----------------------------------

Using docker, you can run commands such as ``apt-get``, ``yum install``, ``pip install`` or ``bundle install`` before running tasks.

Built environment is cached as a new image of Docker.

.. code-block:: yaml

    _export:
      docker:
        image: ubuntu:14.04
        build:
          - apt-get update
          - apt-get install -y python-numpy
          - pip install -r requirements.txt
    
    +step1:
      py>: tasks.MyWorkflow.step1


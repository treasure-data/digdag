Language API - Ruby
==================================

Programmable workflow in Ruby
----------------------------------

workflow.dig:

.. code-block:: yaml

    _export:
      rb:
        require: 'tasks/my_workflow'

    +step1:
      rb>: MyWorkflow.step1

    +step2:
      rb>: MyWorkflow.step2

tasks/my_workflow.rb:

.. code-block:: ruby

    class MyWorkflow
      def step1
        puts "step1"
      end

      def step2
        puts "step2"
      end
    end

Defining variables
----------------------------------

.. code-block:: ruby

    class MyWorkflow
      def step1
        Digdag.env.store(my_value: 1)
      end

      def step2
        puts "step2: %s" % Digdag.env.params['my_value']
      end
    end

Method argument mapping
----------------------------------

.. code-block:: ruby

    class MyWorkflow
      def step1
        Digdag.env.store(my_value: 1)
      end

      def step2(my_value: "default")
        puts "step2: %s" % my_value
      end
    end

Generating child tasks
----------------------------------

Generating Ruby child tasks:

.. code-block:: ruby

    class MyWorkflow
      def step1
        Digdag.env.add_subtask(MyWorkflow, :step3, arg1: 1)
      end

      def step2(my_value: "default")
        puts "step2: %s" % my_value
      end

      def step3(arg1:)
        puts "step3: %s" % arg1
      end
    end

Generating other child tasks:

.. code-block:: ruby

    class MyWorkflow
      def step1
        Digdag.env.add_subtask({
          '_type' => 'mail',
          'body' => 'this is email body in string',
          'subject' => 'workflow started',
          'to' => ['me@example.com'],
        })
      end
    end

.. note::

    Arguments need to be serializable using JSON. If arguments include non-serializable objects such as Proc, add_subtask throws ArgumentError.


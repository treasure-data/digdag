
module Digdag
  require 'forwardable'

  class TaskDispatcher
    def initialize(queues)
      @queues = queues
    end

    def enqueue(action)
      queue_name = action.config.param(:queue, :string, default: "default")
      queue = @queues[queue_name]
      queue.submit(action)
    end
  end

  class SessionApi
    extend Forwardable

    def initialize(session_store)
      @session_store = session_store
    end

    def_delegators :@session_store, :find, :task_finished
  end

end

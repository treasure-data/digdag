require 'yaml'

require 'fluent/log'
class DigdagLogger < Fluent::Log
  def event(level, args)
    time = Time.now
    message = ''
    map = {}
    args.each {|a|
      if a.is_a?(Hash)
        a.each_pair {|k,v|
          map[k.to_s] = v
        }
      else
        message << a.to_s
      end
    }

    map.each_pair {|k,v|
      message << " #{k}=#{v.inspect}"
    }

    return time, message
  end
end

LOG = DigdagLogger.new(STDOUT, level=Fluent::Log::LEVEL_TRACE).enable_color

require 'digdag/config'
require 'digdag/workflow_compiler'
require 'digdag/workflow_store'
require 'digdag/session_store'
require 'digdag/task_dispatcher'
require 'digdag/yaml_workflow_loader'
require 'digdag/yaml_liquid_renderer'
require 'digdag/local_thread_queue'
require 'digdag/action_runner'
require 'digdag/task'

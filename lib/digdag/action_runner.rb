
module Digdag

  class ActionRunner
    def initialize(session_api)
      @session_api = session_api
    end

    def run(action)
      begin
        config = Config.new(action.config)
        state = Config.new(action.state_params)
        params = Config.new(action.params)

        LOG.info "Running action #{action.full_name}: #{config.to_json} state: #{state.to_json} params: #{params.to_json}"

        unless config.has_key?(:type)
          k, v = config.find {|k,v| k.to_s =~ /\>$/ }
          if k
            config[:type] = k[0, k.length-1]
            config[:command] = v
          end
        end

        type = config.param(:type, :string)
        task_class = Plugin.task_class(type)
        task = task_class.new(config, params, state)
        begin
          carry_params, subtask_config = task.call
        ensure
          state_params = task.state rescue state
          state = Config.new(state_params || {})
        end
        error = nil
      rescue => e
        error = e
      end

      carry_params ||= {}
      subtask_config ||= {}

      @session_api.task_finished(action.task_id, state, carry_params, subtask_config, error)
    end
  end

end

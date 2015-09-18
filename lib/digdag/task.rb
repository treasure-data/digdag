
module Digdag

  class PluginManager
    def initialize
      @types = {}
    end

    def register_task(type, task)
      @types[type.to_sym] = task
    end

    def task_class(type)
      k = @types[type.to_sym]
      unless k
        raise ConfigError, "Type #{type.to_s.dump} is not available"
      end
      k
    end
  end

  Plugin = PluginManager.new

  class BaseTask
    def initialize(config, params, state)
      @config = config
      @state = state
      @sub = {}
      @params = params
      @carry_params = {}

      @retry_limit = config.param(:retry_limit, :integer, default: 0)

      init
    end

    attr_reader :config, :params, :state, :carry_params, :sub

    def init
    end

    def call
      retry_control = RetryControl.prepare(@config, @state)
      begin
        run
      rescue => e
        state, interval = retry_control.evaluate(e)
        @state.merge!(state)
        if interval
          raise RetryLaterError.new(e.to_s, interval)
        else
          raise e
        end
      end

      return @carry_params, @sub
    end

    def run
      raise "#{self.class}#run is not implemented"
    end
  end

  # a special error caught by ActionRunner
  class RetryLaterError < RuntimeError
    def self.without_error(interval)
      new(nil, interval)
    end

    def initialize(cause, interval)
      super(cause.to_s)
      @cause = cause
      @interval = interval
    end

    attr_reader :cause, :interval
  end

  class RetryControl
    def self.prepare(config, state, defaults={})
      config = Config.new(config)  # needs Config api (no dup)
      new(
        state,
        config.param(:retry, :integer, default: defaults[:retry] || 3),
        config.param(:retry_wait, :integer, default: defaults[:retry_wait] || 1),
        config.param(:max_retry_wait, :integer, default: defaults[:retry_wait] || 1))
    end

    def initialize(state, retry_limit, initial_retry_wait, max_retry_wait)
      @state = Config.new(state)  # dup
      @retry_count = @state.param(:retry_count, :integer, default: 0)
      @retry_limit = retry_limit
      @initial_retry_wait = initial_retry_wait
      @max_retry_wait = max_retry_wait
    end

    def evaluate(error)
      # TODO unretryable error checks
      if @retry_count >= @retry_limit
        # giveup
        return @state, nil
      else
        retry_wait = @retry_count
        @state[:retry_count] = @retry_count + 1
        interval = [@initial_retry_wait * (2 ** @retry_count), @max_retry_wait].min
        @state[:last_run_at] = Time.now.to_s
        #@state[:last_error] = error.to_s
        return @state, interval
      end
    end
  end

end

require 'digdag/tasks/sh'
require 'digdag/tasks/td_base'
require 'digdag/tasks/td_ddl'
require 'digdag/tasks/td'
require 'digdag/tasks/py'

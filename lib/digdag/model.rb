require 'digdag/jsonable'

module Digdag
  class Model < Jsonable::Value
  end

  class Task < Model
    json_attr :index
    json_attr :name
    json_attr :parent_task_index  # null if root
    #json_attr :child_task_indexes
    json_attr :upstream_task_indexes
    json_attr :grouping_only
    json_attr :config

    # task runner config
    #json_attr :retry_limit
    #json_attr :retry_interval
    #json_attr :max_retry_interval
    #json_attr :after_past
    #json_attr :execution_timeout
  end

  class Workflow < Model
    json_attr :name
    json_attr :meta
    json_attr :tasks, [Task]

    def root_task
      tasks[0]
    end
  end

  class WorkflowModel < Model
    # TODO ActiveRecord model
    json_attr :id
    json_attr :name
  end

  class WorkflowVersion < Model
    # TODO ActiveRecord model
    json_attr :id
    json_attr :name
    json_attr :workflow_id
    json_attr :uuid
    json_attr :meta
    json_attr :tasks, [Task]

    def root_task
      tasks[0]
    end
  end

  class Event < Model
    # TODO ActiveRecord
    json_attr :id
    json_attr :session_params
    json_attr :session_config
  end

  class Session < Model
    # TODO ActiveRecord
    json_attr :id
    json_attr :event_id  # TODO unnecessary?
    json_attr :session_params  # TODO copied from Event
    json_attr :session_config  # this should be statically typed
    json_attr :workflow_version_id
  end

  class Action < Model
    json_attr :task_id
    json_attr :config   # {type: ...}
    json_attr :params
    json_attr :state_params
    json_attr :full_name

    def config
      Config.new(@config)
    end
  end

  class SessionTask < Model
    # TODO ActiveRecord model
    # TODO store mutable fields in a different table (state, retry_at, state_params, carry_params, error, inputs, outputs)
    json_attr :id
    json_attr :task_name
    json_attr :session_id
    json_attr :source_task_index  # nullable if subtask
    json_attr :parent_id
    json_attr :upstream_ids
    json_attr :grouping_only
    json_attr :config
    json_attr :state          # :blocked, :ready, :retry_waiting, :running, :planned, :child_error, :error, :success, :canceled
    json_attr :retry_at
    json_attr :state_params   # retry_count, retry_interval, etc.
    json_attr :carry_params
    json_attr :ignore_parent_error
    json_attr :inputs
    json_attr :outputs
    json_attr :error
    #json_attr :retry_at

    def can_run_children?(ignore_error)
      if ignore_error
        [:error, :child_error, :retry_waiting, :planned, :success].include?(state)
      else
        [:planned, :success].include?(state)
      end
    end

    def can_run_downstream?
      [:success].include?(state)
    end

    def ready?
      @state == :ready
    end

    def planned?
      @state == :planned
    end

    def child_error?
      @state == :child_error
    end

    def success?
      @state == :success
    end

    def error?
      @state == :error
    end

    def subtask?
      @source_task_index == nil
    end

    def ready!
      if @state == :blocked || @state == :retry_waiting || @state == :error || @state == :child_error
        @state = :ready
        @retry_at = nil
      else
        raise "Invalid state transition from #{@state} to :ready"
      end
    end

    def grouping_only_ready!
      if @state == :blocked || @state == :retry_waiting || @state == :error || @state == :child_error
        @state = :planned
      else
        raise "Invalid state transition from #{@state} to :ready"
      end
    end

    def started!
      if @state == :ready
        @state = :running
      else
        raise "Invalid state transition from #{@state} to :running"
      end
    end

    def subtask_override!
      @state = :canceled  # async state change
    end

    def plan_failed!(state_params, carry_params, error)
      if @state == :running
        @state_params = state_params.dup
        @carry_params.merge!(carry_params)
        @error = error.to_s
        @state = :error
      else
        raise "Invalid state transition from #{@state} to :error"
      end
    end

    def plan_retry_later!(state_params, carry_params, error, retry_interval)
      if @state == :running
        @state_params = state_params.dup
        @carry_params.merge!(carry_params)
        @retry_at = Time.now + retry_interval
        @error = error.nil? ? nil : error.to_s
        @state = :retry_waiting
      else
        raise "Invalid state transition from #{@state} to :retry_waiting"
      end
      @retry_at
    end

    def plan_succeeded!(state_params, carry_params, inputs, outputs)
      if @state == :running
        @state_params = state_params.dup
        @carry_params.merge!(carry_params)
        @inputs = inputs
        @outputs = outputs
        @state = :planned
      else
        raise "Invalid state transition from #{@state} to :planned"
      end
    end

    def propagate_children_failure!(state_params, children_errors)
      if @state == :planned
        @error = children_errors.to_json
        @state = :child_error
        @state_params.merge!(state_params)
      else
        raise "Invalid state transition from #{@state} to :child_error"
      end
    end

    def propagate_children_failure_with_retry!(state_params, children_errors, retry_interval)
      if @state == :planned
        @error = children_errors.to_json
        @state = :retry_waiting
        if retry_interval
          @retry_at = Time.now + retry_interval
        end
        @state_params.merge!(state_params)
      else
        raise "Invalid state transition from #{@state} to :retry_waiting"
      end
    end

    def propagate_children_success!
      if @state == :planned
        @state = :success
      else
        raise "Invalid state transition from #{@state} to :success"
      end
    end

    def done?
      [:error, :child_error, :success, :canceled].include?(state)
    end
  end
end

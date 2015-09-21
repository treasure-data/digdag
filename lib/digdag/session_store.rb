
module Digdag
  require 'monitor'
  require 'serverengine'
  require 'concurrent'

  class SessionStore
    def initialize(workflow_store, renderer)
      @workflow_store = workflow_store
      @events = []
      @sessions = []
      @session_tasks = []
      @renderer = renderer

      @enqueue_threads = Concurrent::ThreadPoolExecutor.new

      @mon = Monitor.new
      @background_updater_stop_flag = ServerEngine::BlockingFlag.new
    end

    def notice_background_update!
      @background_updater_stop_flag.instance_eval { @cond }.broadcast if @background_updater_stop_flag
    end

    def start_background_updater(task_dispatcher)
      @background_updater = Thread.new do
        begin
          begin
            while update_state!
              enqueue_ready_tasks!(task_dispatcher)
            end
          end until @background_updater_stop_flag.wait_for_set(1)
        rescue => e
          STDERR.puts "Unexpected error: #{e}"
          e.backtrace.each do |bt|
            STDERR.puts "  #{bt}"
          end
        end
      end
    end

    def trigger_workflow(session_params, workflow_version_ids, session_config)
      @mon.synchronize do
        # do in a transaction

        event = Event.new({
          id: @events.size,
          session_params: session_params,
        })
        @events << event
        retval = []

        workflow_version_ids.each do |workflow_version_id|
          wfv = @workflow_store.find(workflow_version_id)

          session = Session.new({
            id: @sessions.size,
            event_id: event.id,
            session_params: session_params,
            session_config: session_config,
            workflow_version_id: wfv.id,  # TODO this should be UUID?
          })
          @sessions << session
          retval << session.id

          LOG.debug "Running workflow #{wfv.name} (#{wfv.meta.map {|k,v| "#{k}:#{v}"}.join(', ')})"
          wfv.tasks.each do |task|
            LOG.debug "  Task[#{task.index}]: #{task.name}"
            LOG.debug "    parent: #{task.parent_task_index}"
            LOG.debug "    upstreams: #{task.upstream_task_indexes.inspect}"
            LOG.debug "    config: #{task.config.to_json}"
          end

          add_session_tasks(nil, [], session.id, wfv.tasks)
        end

        retval
      end
    end

    def update_state!
      @mon.synchronize do
        changed = false
        @session_tasks.each do |st|
          if update_task_state(st)
            changed = true
          end
        end
        changed
      end
    end

    def enqueue_ready_tasks!(dispatcher)
      @mon.synchronize do
        @session_tasks.each do |st|
          if st.ready?

            st.started!  # TODO use transaction. lock the entry here

            @enqueue_threads.post do
              begin
                full_name = collect_full_task_name(st)

                session_config = Config.new(@sessions[st.session_id].session_config || {})
                skip_tasks = session_config.param(:skip_tasks, :hash, default: {})
                skip_data = skip_tasks[full_name]

                if skip_data
                  LOG.info "Skipping #{full_name}"
                  task_finished(st.id, {}, skip_data['carry_params'], nil, nil, skip_data['inputs'], skip_data['outputs'])
                else
                  session_params = @sessions[st.session_id].session_params
                  params = session_params.merge(collect_action_params(st))
                  config = @renderer.render(st.config, params)
                  action = Action.new({
                    task_id: st.id,
                    config: config,
                    params: params,
                    state_params: st.state_params,
                    full_name: full_name,
                  })
                  dispatcher.enqueue(action)
                end
              rescue => e
                state_params = {"schedule_error" => true}
                task_finished(st.id, state_params, {}, nil, e, [], [])
              end
            end
            # TODO use transaction. commit here.

          end
        end
      end
    end

    def task_finished(task_id, state_params, carry_params, subtask_config, error, inputs, outputs)
      @mon.synchronize do
        if error
          LOG.warn "Task failed [#{task_id}]: #{error}"
          if error.respond_to?(:backtrace)
            error.backtrace.each {|bt| LOG.warn "  #{bt}" }
          end
        else
          LOG.warn "Task finished [#{task_id}]: carry_params=#{carry_params.to_json}"
        end
        st = find(task_id)

        if error
          if error.is_a?(RetryLaterError)
            st.plan_retry_later!(state_params, carry_params, error.cause, error.interval)  # error.cause can be nil
          else
            st.plan_failed!(state_params, carry_params, error)
          end
        else
          st = find(task_id)
          if subtask_config && !subtask_config.empty?
            subtask = WorkflowCompiler.compile_tasks(".sub", Config.new(subtask_config))
            LOG.debug "Adding subtask: #{subtask.inspect}"
            if subtask && !subtask.empty?
              substs = add_session_tasks(st.id, [], st.session_id, subtask)
              subst = substs[0]  # root subtask
            end
          end
          check_config = Config.new(st.config).param(:check, :hash, default: {})  # TODO parse at WorkflowCompiler?
          unless check_config.empty?
            check_task = WorkflowCompiler.compile_tasks(".check", Config.new(check_config))
            if check_task && !check_task.empty?
              root_upstream_ids = [subst.nil? ? nil : subst.id].compact
              add_session_tasks(st.id, root_upstream_ids, st.session_id, check_task, no_delete_existent: true)
            end
          end
          st.plan_succeeded!(state_params, carry_params, inputs, outputs)
          # possible optimization: set state=success if this task doesn't have subtask
        end

        notice_background_update!
      end
    end

    def any_runnable?
      @mon.synchronize do
        @session_tasks.any? {|st| collect_is_progressible(st) }
      end
    end

    def all_done?
      @mon.synchronize do
        @session_tasks.all? {|st| st.done? }
      end
    end

    def any_failed?
      @mon.synchronize do
        @session_tasks.any? {|st| st.error? }
      end
    end

    def find(id)
      @session_tasks[id]
    end

    def collect_success_tasks(session_ids)
      success_tasks = @session_tasks.select do |st|
        session_ids.include?(st.session_id) && (st.planned? || st.success?)  # || st.child_error?
      end
      kvs = success_tasks.map do |st|
        [collect_full_task_name(st), {"state" => st.state.to_s, "carry_params" => st.carry_params, "inputs" => (st.inputs || []), "outputs" => (st.outputs || [])}]
      end
      Hash[kvs]
    end

    def collect_dot_graph
      builders = []
      @session_tasks.each_with_index do |st,i|
        builder = builders[i] = DotGraphBuilder.new(st.id, st.task_name, st)
        st.upstream_ids.each do |up_id|
          builder.add_upstream(builders[up_id])
        end
        if st.parent_id
          builders[st.parent_id].add_child(builder)
        end
      end
      require 'stringio'
      out = StringIO.new
      root = builders[0]
      out.puts "digraph session {"
      out.puts "compound=true"
      root.build!(out)
      out.puts "}"
      out.string
    end

    class DotGraphBuilder
      def initialize(id, name, st)
        @id = id
        @name = name
        @st = st
        @children = []
        @upstreams = []
      end

      attr_reader :name, :id, :upstreams, :children

      def add_child(builder)
        @children << builder
      end

      def add_upstream(builder)
        @upstreams << builder
      end

      def build!(out)
        if @children.empty?
          out.puts "\"#{@id}\" [label=\"#{@name}\" style=filled fillcolor=#{state_color}]"
        else
          out.puts "subgraph \"cluster_#{@id}\" {"
          out.puts "label = \"#{@name}\""
          out.puts "style=filled"
          out.puts "bgcolor=#{state_color}"
          @children.each_with_index do |b,i|
            b.build!(out)
            b.upstreams.each do |c|
              opts = []

              if c.children.empty?
                src = c.id
              else
                src = first_node_id(c)
                opts << "ltail=\"cluster_#{c.id}\""
              end
              if b.children.empty?
                dst = b.id
              else
                dst = first_node_id(b)
                opts << "lhead=\"cluster_#{b.id}\""
              end

              if opts.empty?
                out.puts "\"#{src}\" -> \"#{dst}\""
              else
                out.puts "\"#{src}\" -> \"#{dst}\" [#{opts.join(' ')}]"
              end
            end
          end
          out.puts "}"
        end
      end

      private

      def state_color
        case @st.state
        when :blocked
          "gray74"
        when :ready
          "gray80"
        when :retry_waiting
          "gray94"
        when :running
          "yellowgreen"
        when :planned
          "slateblue1"
        when :error
          "indianred1"
        when :child_error
          "yellow"
        when :success
          "olivedrab3"
        when :canceled
          "graph50"
        end
      end

      def first_node_id(c)
        if c.children.empty?
          return c.id
        else
          return first_node_id(c.children.first)
        end
      end
    end

    private

    def add_session_tasks(root_parent_id, root_upstream_ids, session_id, tasks, options={})
      if root_parent_id && !options[:no_delete_existent]
        # delete previously submitted subtasks
        @session_tasks.each_with_index do |st,i|
          if collect_is_child_of(st, root_parent_id)
            # assuming that grouping task (which has subtasks by definition) doesn't create subtasks programabbly
            @session_tasks[i].subtask_override!
          end
        end
      end

      index_id_map = []
      tasks.map do |task|
        session_task = SessionTask.new({
          id: @session_tasks.size,
          task_name: task.name,
          session_id: session_id,
          source_task_index: task.index,
          parent_id: (task.parent_task_index.nil? ? root_parent_id : index_id_map[task.parent_task_index]),
          upstream_ids: (task.parent_task_index.nil? ? root_upstream_ids : []) + task.upstream_task_indexes.map {|index| index_id_map[index] },
          grouping_only: task.grouping_only,
          config: task.config,
          state: :blocked,
          state_params: {},
          carry_params: {},
        })
        @session_tasks << session_task
        index_id_map[task.index] = session_task.id
        session_task
      end
    end

    def update_task_state(st)
      case st.state
      when :blocked, :retry_waiting
        if ready_to_start?(st)
          if st.grouping_only
            st.grouping_only_ready!
          else
            st.ready!
          end
          return true
        end

      when :planned
        is_all_done, errors = collect_children_state(st)
        if is_all_done
          retry_control = RetryControl.prepare(st.config, st.state_params, retry: 0)  # default group retry count is 0
          if errors.empty?
            st.propagate_children_success!
          else
            state_params, retry_interval = retry_control.evaluate(errors)
            if retry_interval
              st.propagate_children_failure_with_retry!(state_params, errors, retry_interval)
            else
              st.propagate_children_failure!(state_params, errors)
            end
          end
          return true
        end
      end

      return nil
    end

    def ready_to_start?(st)
      parent = find(st.parent_id) if st.parent_id
      if (st.retry_at.nil? || st.retry_at <= Time.now) && (parent.nil? || parent.can_run_children?)
        # subtasks in this group is ready to start
        return st.upstream_ids.all? {|i| find(i).can_run_downstream? }
      else
        return false
      end
    end

    def collect_children_state(st, errors=[])
      is_all_done = true
      @session_tasks.each do |s|
        if s.parent_id == st.id
          errors << s.error if s.error
          if collect_is_progressible(s)
            is_all_done = false
          end
        end
      end
      return is_all_done, errors
    end

    def collect_action_params(st)
      collect_parent_carry_params(st).merge(collect_upstream_carry_params(st))
    end

    def collect_upstream_carry_params(st)
      params = {}
      st.upstream_ids.each do |id|
        up = find(id)
        up_params = collect_action_params(up).merge(collect_task_params(up))
        params.merge!(up_params)
      end
      params
    end

    def collect_parent_carry_params(st)
      parent = find(st.parent_id) if st.parent_id
      if parent
        collect_action_params(parent).merge(collect_task_params(parent))
      else
        {}
      end
    end

    def collect_task_params(st)
      params = Config.new(st.config).param(:params, :hash, default: {})  # TODO should be parsed at WorkflowCompiler and set to Task and SessionTask as :task_params?
      params.merge(st.carry_params)
    end

    def collect_full_task_name(st)
      parent = find(st.parent_id) if st.parent_id
      if parent
        return collect_full_task_name(parent) + st.task_name
      else
        st.task_name
      end
    end

    def collect_is_progressible(st)
      return false if st.done?

      parent = find(st.parent_id) if st.parent_id
      if parent
        # progressible if parent is not done, or parent is done but can run children
        if collect_is_progressible(parent) || parent.can_run_children?
          return st.upstream_ids.all? do |up_id|
            # progressible if dependent is not done, or dependent is successfully done
            up = find(up_id)
            collect_is_progressible(up) || up.can_run_downstream?
          end
        end
        return false
      else
        return true
      end
    end

    def collect_is_child_of(st, parent_id)
      parent = find(st.parent_id) if st.parent_id
      if parent
        if parent.id == parent_id
          return true
        else
          return collect_is_child_of(parent, parent_id)
        end
      else
        return false
      end
    end
  end

end

require 'digdag/model'

module Digdag

  class WorkflowCompiler
    class TaskBuilder
      def initialize(index, parent, name, grouping_only, config)
        @index = index
        @parent = parent
        @name = name
        @children = []
        @upstreams = []
        @grouping_only = grouping_only
        @config = config
        parent.add_child(self) if parent
      end

      attr_reader :index, :name, :parent, :children, :upstreams, :grouping_only, :config

      def add_upstream(desc)
        @upstreams << desc
      end

      def build
        Task.new({
          index: @index,
          name: @name,
          parent_task_index: (@parent.nil? ? nil : @parent.index),
          #child_task_indexes: children.map {|child| child.index },
          upstream_task_indexes: resolve_upstream_task_indexes,
          grouping_only: @grouping_only,
          config: @config,
        })
      end

      def add_child(task)
        @children << task
      end

      private

      def resolve_upstream_task_indexes
        # TODO detect circular dependency
        @upstreams.map do |up|
          if up.is_a?(TaskBuilder)
            up.index
          else
            bros = @parent.nil? ? [] : @parent.children
            depend = bros.find {|bro| bro.name == up }
            unless depend
              raise "Dependency '#{up}' does not exist"
            end
            depend.index
          end
        end.uniq
      end
    end

    def self.compile(name, workflow_config)
      new(name, workflow_config).compile!
    end

    def self.compile_tasks(name, workflow_config)
      new(name, workflow_config).compile_tasks!
    end

    def initialize(name, workflow_config)
      @workflow_name = name
      @workflow_config = workflow_config
      @task_index = 0
      @tasks = []
    end

    def compile!
      Workflow.new({
        name: @workflow_name,
        meta: @workflow_config.param(:meta, :hash, default: {}),
        tasks: compile_tasks!,
      })
    end

    def compile_tasks!
      collect(nil, {}, @workflow_name, @workflow_config)
      @tasks.map {|task| task.build }
    end

    private

    def collect(parent_task, parent_default_config, name, config)
      default_config = config.param(:default, :hash, default: {})
      config = config.merge(parent_default_config)
      next_default_config = parent_default_config.merge(default_config)

      subtask_keys = config.keys.select {|key| key =~ /^\+/ }
      task_config_keys = config.keys.select {|key| key !~ /^\+/ }
      task_config = Hash[task_config_keys.map {|k| [k, config[k]] }]

      if config.has_key?(:type) || config.keys.any? {|k| k.to_s =~ /\>$/ }
        # task node
        unless subtask_keys.empty?
          raise ConfigError, "A task can't have subtasks: #{config.simple_to_s}"
        end
        task = add_task(parent_task, name, false, task_config)

      else
        # group node
        task = add_task(parent_task, name, true, task_config)

        subtasks = subtask_keys.map do |key|
          collect(task, next_default_config, key, config.param(key, :hash))
        end

        if config.param(:parallel, :boolean, default: false)
          # after: option works only for parallel: true
          names = {}
          subtasks.each do |cur|
            Config.new(cur.config).param(:after, [:string], default: []).each do |dep|
              up = names[dep]
              unless up
                raise ConfigError, "Dependency task #{dep.to_s.dump} does not exist"
              end
              cur.add_upstream(up)
            end
            names[cur.name] = cur
          end
        else
          # generate after: option if parallel: false
          before = nil
          subtasks.each do |cur|
            cur.add_upstream(before) if before
            before = cur
          end
        end
      end

      task
    end

    def add_task(*args)
      task = TaskBuilder.new(@task_index, *args)
      @tasks << task
      @task_index += 1
      task
    end
  end

end

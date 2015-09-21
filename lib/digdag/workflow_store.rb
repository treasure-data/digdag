
module Digdag
  require 'monitor'
  require 'uuidtools'

  class WorkflowStore
    def initialize
      @workflow_models = {}
      @versions = []

      @mon = Monitor.new
    end

    def create_version(workflow)
      @mon.synchronize do
        wfm = @workflow_models[workflow.name]
        unless wfm
          wfm = @workflow_models[workflow.name] = WorkflowModel.new({
            id: @workflow_models.size,
            name: workflow.name
          })
        end
        version = WorkflowVersion.new({
          id: @versions.size,
          name: workflow.name,
          workflow_id: wfm.id,
          uuid: UUIDTools::UUID.random_create,
          meta: workflow.meta,
          tasks: workflow.tasks,
        })
        @versions << version
        version
      end
    end

    def latest_version_by_name(name)
      @versions.reverse.find {|wf| wf.name == name }
    end

    def find(id)
      @versions[id]
    end
  end
end

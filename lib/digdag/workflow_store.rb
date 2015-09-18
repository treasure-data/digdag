
module Digdag
  require 'uuidtools'

  class WorkflowStore
    def initialize
      @versions = []
    end

    def create_version(workflow)
      version = WorkflowVersion.new({
        id: @versions.size,
        uuid: UUIDTools::UUID.random_create,
        name: workflow.name,
        meta: workflow.meta,
        tasks: workflow.tasks,
      })
      @versions << version
      version
    end

    def latest_version_by_name(name)
      @versions.reverse.find {|wf| wf.name == name }
    end

    def find(id)
      @versions[id]
    end
  end
end


module Digdag
  module Tasks

    class CommandTask < BaseTask
      Plugin.register_task(:sh, self)
      Plugin.register_task(:command, self)

      def run
        command = config.param(:command, :string)

        success = system(command)
        unless success
          raise "Command failed"
        end
      end

      def preview
        nil
      end
    end

  end
end

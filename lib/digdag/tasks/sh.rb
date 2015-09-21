
module Digdag
  module Tasks

    class CommandTask < BaseTask
      Plugin.register_task(:sh, self)
      Plugin.register_task(:command, self)

      def run
        command = config.param(:command, :string)

        env = {}
        params.each_pair do |k,v|
          if v.is_a?(String)
            env[k.to_s] = v
          else
            env[k.to_s] = v.to_json
          end
        end

        success = system(env, command)
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

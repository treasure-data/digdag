
module Digdag
  module Tasks
    require 'tempfile'

    class PyTask < BaseTask
      Plugin.register_task(:py, self)

      def run
        in_file = Tempfile.new('py-in-')
        out_file = Tempfile.new('py-out-')

        if command = config.param(:command, :string, default: nil)
          fragments = command.split(".")
          klass = fragments.pop
          package = fragments
          script = ""
          script << "from #{package.join('.')} import #{klass}\n" unless package.empty?
          script << <<EOF
task = #{klass}(config, state, params)
task.run()

out = dict()
if hasattr(task, 'sub'):
    out['sub'] = task.sub
if hasattr(task, 'carry_params'):
    out['carry_params'] = task.carry_params
with open(out_file, 'w') as out_file:
    json.dump(out, out_file)
EOF

          config[:script] = script
        end

        script = config.param(:script, :string)

        code = <<EOF
import json
in_file = #{in_file.path.dump}
out_file = #{out_file.path.dump}
with open(in_file) as f:
    in_data = json.load(f)
    config = in_data['config']
    params = in_data['params']
    state = in_data['state']

#{script}
EOF

        #LOG.trace "Python code ---------------------"
        #LOG.trace code
        #LOG.trace "---------------------------------"

        in_file.write({
          config: @config,
          params: @params,
          state: @state,
        }.to_json)
        in_file.flush

        message = nil
        IO.popen(["python", "-", err: [:child, :out]], "r+", ) do |io|
          io.write(code)
          io.close_write
          message = io.read
        end
        STDOUT.write message if message
        unless $?.success?
          raise "Python command failed: #{message}"
        end

        data = out_file.read
        unless data.empty?
          out = Config.new(JSON.parse(data))
          @sub = out.param(:sub, :hash, default: {})
          @carry_params = out.param(:carry_params, :hash, default: {})
        end
      end
    end

  end
end

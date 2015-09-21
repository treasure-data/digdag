
module Digdag
  module Tasks
    require 'tempfile'

    class PyTask < BaseTask
      Plugin.register_task(:py, self)

      #def preview
      #end

      def run
        data = run_code("run")

        unless data.empty?
          out = Config.new(JSON.parse(data))
          @sub = out.param(:sub, :hash, default: {})
          @carry_params = out.param(:carry_params, :hash, default: {})
          @inputs = out.param(:inputs, :array, default: [])
          @outputs = out.param(:outputs, :array, default: [])
        end
      end

      private

      def run_code(method_name)
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
EOF
          if method_name
            script << "task.#{method_name}()\n"
          end

          script << <<EOF
out = dict()
if hasattr(task, 'sub'):
    out['sub'] = task.sub
if hasattr(task, 'carry_params'):
    out['carry_params'] = task.carry_params
if hasattr(task, 'inputs'):
    out['inputs'] = task.inputs  # TODO check callable
if hasattr(task, 'outputs'):
    out['outputs'] = task.outputs  # TODO check callable
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

        return out_file.read
      end
    end

  end
end

require 'json'

command = ARGV[0]
out_file = ARGV[2]

module DigdagEnv
  in_file = ARGV[1]

  in_data = JSON.parse(File.read(in_file))
  config = in_data['config']

  CONFIG = config
  SUBTASK_CONFIG = {}
  STATE_PARAMS = {}
  EXPORT_PARAMS = {}
  CARRY_PARAMS = {}
end

# TODO move this to digdag.gem
module Digdag
  def self.config
    DigdagEnv::CONFIG
  end

  def self.subtask_config
    DigdagEnv::SUBTASK_CONFIG
  end

  def self.state_params
    DigdagEnv::STATE_PARAMS
  end

  def self.export_params
    DigdagEnv::EXPORT_PARAMS
  end

  def self.carry_params
    DigdagEnv::CARRY_PARAMS
  end
end

# add current directory to LOAD_PATH
$LOAD_PATH << File.expand_path(Dir.pwd)

def digdag_inspect_command(command)
  # Name::Space::Class.method
  fragments = command.split(".")
  method_name = fragments.pop.to_sym
  class_name = fragments.join(".")
  clazz = Kernel.const_get(class_name)
  is_instance_method = clazz.public_instance_methods.include?(method_name)
  return clazz, method_name, is_instance_method
end

clazz, method_name, is_instance_method = digdag_inspect_command(command)

if is_instance_method
  instance = clazz.new(Digdag.config)
  result = instance.send(method_name, Digdag.config)

else
  result = clazz.send(method_name, Digdag.config)
end

out = {
  'subtask_config' => Digdag.subtask_config,
  'export_params' => Digdag.export_params,
  'state_params' => Digdag.state_params,
  'carry_params' => Digdag.carry_params,
}

File.open(out_file, "w") {|f| f.write out.to_json }


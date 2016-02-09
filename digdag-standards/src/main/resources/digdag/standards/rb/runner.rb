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

def digdag_inspect_arguments(receiver, method_name, config)
  parameters = receiver.method(method_name).parameters

  if method_name == :new && parameters == [[:rest]]
    # This is Object.new that forwards all arguments to #initialize
    begin
      parameters = receiver.instance_method(:initialize).parameters
    rescue NameError => e
    end
  end

  args = []
  keywords = nil
  parameters.each do |kind,name|
    key = name.to_s
    case kind
    when :req
      # required argument like a
      unless config.has_key?(key)
        if receiver.is_a?(Class)
          raise ArgumentError, "Method '#{receiver}.#{method_name}' requires parameter '#{key}' but not set"
        else
          raise ArgumentError, "Method '#{receiver.class}##{method_name}' requires parameter '#{key}' but not set"
        end
      end
      args << config[key]

    when :opt
      # optional argument like a=nil
      if config.has_key?(key)
        args << config[key]
      else
        # use the default value.
      end

    when :rest
      # variable-length arguments like *a
      # there're really we can do here to keep consistency with :opt.
      # should this be an error?

    when :keyreq
      # required keyword argument like a:
      unless config.has_key?(key)
        if receiver.is_a?(Class)
          raise ArgumentError, "Method '#{receiver}.#{method_name}' requires parameter '#{key}' but not set"
        else
          raise ArgumentError, "Method '#{receiver.class}##{method_name}' requires parameter '#{key}' but not set"
        end
      end
      if keywords.nil?
        keywords = {}
        args << keywords
      end
      keywords[name] = config[key]

    when :key
      # optional keyword argument like a: nil
      if config.has_key?(key)
        if keywords.nil?
          keywords = {}
          args << keywords
        end
        keywords[name] = config[key]
      else
        # use the default value.
      end

    when :keyrest
      # rest-of-keywords argument like **a
      # symbolize keys otherwise method call causes error:
      # "TypeError: wrong argument type String (expected Symbol)"
      if keywords.nil?
        keywords = {}
        args << keywords
      end
      keywords.merge!(digdag_symbolize_keys(config))
    end
  end

  return args
end

def digdag_symbolize_keys(hash)
  built = {}
  hash.each_pair do |k,v|
    if v.is_a?(Hash)
      v = digdag_symbolize_keys(v)
    end
    built[k.to_s.to_sym] = v
  end
  return built
end

clazz, method_name, is_instance_method = digdag_inspect_command(command)

if is_instance_method
  new_args = digdag_inspect_arguments(clazz, :new, Digdag.config)
  instance = clazz.new(*new_args)

  method_args = digdag_inspect_arguments(instance, method_name, Digdag.config)
  result = instance.send(method_name, *method_args)

else
  method_args = digdag_inspect_arguments(clazz, method_name, Digdag.config)
  result = clazz.send(method_name, *method_args)
end

out = {
  'subtask_config' => Digdag.subtask_config,
  'export_params' => Digdag.export_params,
  'state_params' => Digdag.state_params,
}

File.open(out_file, "w") {|f| f.write out.to_json }


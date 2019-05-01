require 'json'

command = ARGV[0]
out_file = ARGV[2]

module DigdagEnv
  in_file = ARGV[1]

  in_data = JSON.parse(File.read(in_file))
  params = in_data['params']

  # TODO include indifferent access like Embulk::DataSource
  PARAMS = params
  SUBTASK_CONFIG = {}
  EXPORT_PARAMS = {}
  STORE_PARAMS = {}
  STATE_PARAMS = {}
end

# should this be a digdag.gem so that users can unit-test a command without running digdag?
module Digdag
  class Env
    def initialize
      @params = DigdagEnv::PARAMS
      @subtask_config = DigdagEnv::SUBTASK_CONFIG
      @export_params = DigdagEnv::EXPORT_PARAMS
      @store_params = DigdagEnv::STORE_PARAMS
      @state_params = DigdagEnv::STATE_PARAMS
      @subtask_index = 0
    end

    attr_reader :params

    attr_reader :subtask_config

    attr_reader :export_params

    attr_reader :store_params

    attr_reader :state_params

    def set_state(**params)
      @state_params.merge!(params)
    end

    def export(**params)
      @export_params.merge!(params)
    end

    def store(**params)
      @store_params.merge!(params)
    end

    # add_subtask(params)
    # add_subtask(singleton_method_name, params={})
    # add_subtask(klass, instance_method_name, params={})
    def add_subtask(*args)
      if args.length == 1 && args[0].is_a?(Hash)
        # add_subtask(params)
        config = args[0]

      elsif args.length == 1 || (args.length == 2 && args[1].is_a?(Hash))
        # add_subtask(singleton_method_name, params={})
        method_name = args[0]
        params = Hash(args[1])

        begin
          method_name = method_name.to_sym
        rescue NameError, ArgumentError
          raise ArgumentError, "Second argument must be a Symbol but got #{method_name.inspect}"
        end

        if method_name.to_s.include?(".")
          raise ArgumentError, "Method name can't include '.'"
        end

        config = params.dup
        config["rb>"] = method_name.to_s

      elsif args.length == 2 || (args.length == 3 && args[2].is_a?(Hash))
        # add_subtask(klass, instance_method_name, params={})
        klass = args[0]
        method_name = args[1]
        params = Hash(args[2])

        begin
          method_name = method_name.to_sym
        rescue NameError, ArgumentError
          raise ArgumentError, "Second argument must be a Symbol but got #{method_name.inspect}"
        end

        if method_name.to_s.include?(".")
          raise ArgumentError, "Method name can't include '.'"
        end

        if klass.is_a?(Class)
          class_name = klass.name

        else
          begin
            class_name = klass.to_sym.to_s
          rescue NameError, ArgumentError
            raise ArgumentError, "First argument must be a Class or Symbol but got #{klass.inspect}"
          end
        end

        # validation
        begin
          klass = Kernel.const_get(class_name)  # const_get with String (not Symbol) searches nested constants
        rescue NameError
          raise ArgumentError, "Could not find class named #{class_name}"
        end

        unless klass.respond_to?(method_name) || klass.public_instance_methods.include?(method_name)
          raise ArgumentError, "Class #{klass} does not have method #{method_name.inspect}"
        end

        config = params.dup
        config["rb>"] = "::#{klass}.#{method_name}"

      else
        raise ArgumentError, "wrong number of arguments (#{args.length} for 1..3 with the last argument is a Hash)"
      end

      begin
        JSON.dump(config)
      rescue => e
        raise ArgumentError, "Parameters must be serializable using JSON: #{e}"
      end

      @subtask_config["+subtask#{@subtask_index}"] = config
      @subtask_index += 1

      nil
    end
  end

  DIGDAG_ENV = Env.new
  private_constant :DIGDAG_ENV

  def self.env
    DIGDAG_ENV
  end
end

# add the archive path to LOAD_PATH
$LOAD_PATH << File.expand_path(Dir.pwd)

def digdag_inspect_command(command)
  fragments = command.split(".")
  method_name = fragments.pop.to_sym
  if fragments.empty?
    # method
    return nil, method_name, false
  else
    # Name::Space::Class.method
    class_name = fragments.join(".")
    klass = Kernel.const_get(class_name)
    is_instance_method = klass.public_instance_methods.include?(method_name)
    return klass, method_name, is_instance_method
  end
end

def digdag_inspect_arguments(receiver, method_name, params)
  if receiver
    parameters = receiver.method(method_name).parameters
    if method_name == :new && parameters == [[:rest]]
      # This is Object.new that forwards all arguments to #initialize
      begin
        parameters = receiver.instance_method(:initialize).parameters
      rescue NameError => e
      end
    end
  else
    parameters = method(method_name).parameters
  end

  args = []
  keywords = nil
  parameters.each do |kind,name|
    key = name.to_s
    case kind
    when :req
      # required argument like a
      unless params.has_key?(key)
        if receiver.is_a?(Class)
          raise ArgumentError, "Method '#{receiver}.#{method_name}' requires parameter '#{key}' but not set"
        else
          raise ArgumentError, "Method '#{receiver.class}##{method_name}' requires parameter '#{key}' but not set"
        end
      end
      args << params[key]

    when :opt
      # optional argument like a=nil
      if params.has_key?(key)
        args << params[key]
      else
        # use the default value.
      end

    when :rest
      # variable-length arguments like *a
      # there're really we can do here to keep consistency with :opt.
      # should this be an error?

    when :keyreq
      # required keyword argument like a:
      unless params.has_key?(key)
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
      keywords[name] = params[key]

    when :key
      # optional keyword argument like a: nil
      if params.has_key?(key)
        if keywords.nil?
          keywords = {}
          args << keywords
        end
        keywords[name] = params[key]
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
      keywords.merge!(digdag_symbolize_keys(params))
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

klass, method_name, is_instance_method = digdag_inspect_command(command)
error = nil

if klass.nil?
  method_args = digdag_inspect_arguments(nil, method_name, DigdagEnv::PARAMS)
  begin
    result = send(method_name, *method_args)
  rescue => error
  end

elsif is_instance_method
  new_args = digdag_inspect_arguments(klass, :new, DigdagEnv::PARAMS)
  instance = klass.new(*new_args)

  method_args = digdag_inspect_arguments(instance, method_name, DigdagEnv::PARAMS)
  begin
    result = instance.send(method_name, *method_args)
  rescue => error
  end

else
  method_args = digdag_inspect_arguments(klass, method_name, DigdagEnv::PARAMS)
  begin
    result = klass.send(method_name, *method_args)
  rescue => error
  end
end

out = {
  'subtask_config' => DigdagEnv::SUBTASK_CONFIG,
  'export_params' => DigdagEnv::EXPORT_PARAMS,
  'store_params' => DigdagEnv::STORE_PARAMS,
  #'state_params' => DigdagEnv::STATE_PARAMS,  # only for retrying
}

if error
  out['error'] = {
    'class' => error.class.to_s,
    'message' => error.message,
    'backtrace' => error.backtrace,
  }
end

File.open(out_file, "w") {|f| f.write out.to_json }

raise error if error

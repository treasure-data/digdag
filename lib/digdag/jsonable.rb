require 'json'
require 'time'

module Jsonable

  class TypeManager
    def initialize
      @registry = {}
    end

    def register_type(types, converter=nil, &block)
      converter ||= block
      Array(types).each do |type|
        @registry[type.to_sym] = converter
      end
      nil
    end

    def convert(type, value, options={})
      if type.is_a?(Array)
        element_type = type[0]
        Array(value).map {|e| convert(element_type, e) }
      elsif type.is_a?(Hash)
        key_type = type.keys.first
        value_type = type[key_type]
        unless value.is_a?(Hash)
          raise ArgumentError, "Hash is required but got #{value.inspect}"
        end
        Hash[ value.map {|k,v| [convert(key_type, k, options), convert(value_type, v, options)] } ]
      else
        converter = @registry[type.to_sym]
        unless converter
          raise ArgumentError, "Unsupported config type #{type.to_sym.inspect}"
        end
        converter.call(value, options)
      end
    end
  end

  Types = TypeManager.new

  Types.register_type(:string) do |value, options|
    String(value)
  end

  Types.register_type(:integer) do |value, options|
    Integer(value)
  end

  Types.register_type(:float) do |value, options|
    Float(value)
  end

  Types.register_type(:time) do |value, options|
    Time.parse(value)
  end

  Types.register_type(:boolean) do |value, options|
    case value
    when true, "true", 1
      true
    when false, "false", 0
      false
    else
      raise ArgumentError, "Boolean is required but got: #{value.inspect}"
    end
  end

  Types.register_type(:hash) do |value, options|
    unless value.is_a?(Hash)
      raise ArgumentError, "Hash is required but got #{value.inspect}"
    end
    value
  end

  Types.register_type(:array) do |value, options|
    Array(value)
  end

  class JsonAttribute
    def initialize(key, type)
      @key = key.to_sym
      @type = type
    end

    attr_reader :key, :type
  end

  class JsonSubtype < JsonAttribute
    def initialize(key)
      super(key, String)
    end
  end

  class Value
    class << self
      def json_local_declared_attributes
        @json_local_declared_attributes ||= []
      end

      def json_declared_attributes
        result = []
        ancestors.reverse_each do |a|
          if a.respond_to?(:json_local_declared_attributes)
            result.concat(a.json_local_declared_attributes)
          end
        end
        result
      end

      def json_declared_subtype
        json_declared_attributes.find {|a| a.is_a?(JsonSubtype) }
      end

      def json_subtype(key)
        json_local_declared_attributes << JsonSubtype.new(key)
        attr_accessor key
      end

      def json_attr(key, type=Object)
        json_local_declared_attributes << JsonAttribute.new(key, type)
        attr_accessor key
      end

      def json_create(object)
        # TODO subtyping
        fields = self.json_declared_attributes.map do |a|
          decode_json_attribute(a.type, object[a.key].to_s)
        end
        new(fields)
      end

      ## TODO subtyping?
      #def new(object)
      #end

      private

      def decode_json_attribute(type, value)
        if type.is_a?(Module)
          if value.is_a?(type)
            value
          else
            type.json_create(src)
          end
        else
          Jsonable::Types.convert(value, type)
        end
      end
    end

    def initialize(fields)
      if fields.is_a?(Array)
        self.class.json_declared_attributes.each_with_index do |a,i|
          instance_variable_set("@#{a.key}", fields[i])
        end
      else
        self.class.json_declared_attributes.each do |a|
          instance_variable_set("@#{a.key}", fields[a.key])
        end
      end
    end

    def to_h
      hash = {}
      self.class.json_declared_attributes.each do |a|
        hash[a.key] = instance_variable_get("@#{a.key}")
      end
      hash
    end

    def to_json
      to_h.to_json
    end
  end

end

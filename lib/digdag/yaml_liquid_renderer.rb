
module Digdag
  require 'liquid'

  class YamlLiquidRenderer
    #class Context < Liquid::Context
    #  def invoke(method, *args)
    #    super.to_json(space: ' ', allow_nan: true)
    #  end
    #end

    module Filters
      def yaml(input)
        js = input.to_json(space: ' ', allow_nan: true)
        if js[0] == "{"
          "<<: #{js}"
        else
          js
        end
      end

      def json(input)
        input.to_json(space: ' ', allow_nan: true)
      end
    end

    def render(config, params)
      config = Config.new(config)  # copy

      include_params = config.param("<<use", [:string], default: nil)
      if include_params
        include_params.each do |k|
          inc = params[k]
          if inc.is_a?(Hash)
            kv = render(Config.new(inc), params)  # TODO prevent stack too deep error
            kv.each {|k,v| config[k] = v }
          end
        end
        config.delete("<<params")
      end

      include_render = config.param("<<render", :string, default: nil)
      if include_render
        #context = Context.new(params)
        yaml = Liquid::Template.parse(include_render).render(params, filters: [Filters])
        kv = YAML.load(yaml)
        kv.each {|k,v| config[k] = v }
        config.delete("<<render")
      end

      config
    end
  end

end

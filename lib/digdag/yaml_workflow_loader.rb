
module Digdag

  class YamlWorkflowLoader
    # returns list of workflows represented by List<Config>
    def load_file(path)
      # TODO liquid
      loaded = YAML.load_file(path)
      Hash[ loaded.map {|k,v| [k, Config.new(v)] } ]
    end
  end

end

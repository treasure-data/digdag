package io.digdag.core.config;

import io.digdag.client.config.ConfigException;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.List;
import java.util.stream.Collectors;

/**
 * A {@link SafeConstructor} that disallows duplicate keys.
 */
class StrictSafeConstructor
        extends SafeConstructor
{
    StrictSafeConstructor()
    {
        this.yamlConstructors.put(Tag.MAP, new StrictConstructYamlMap());
    }

    private class StrictConstructYamlMap
            extends StrictSafeConstructor.ConstructYamlMap
    {
        @Override
        public Object construct(Node node)
        {
            if (node instanceof MappingNode) {
                validateKeys((MappingNode) node);
            }
            return super.construct(node);
        }

        @Override
        public void construct2ndStep(Node node, Object object)
        {
            if (node instanceof MappingNode) {
                validateKeys((MappingNode) node);
            }
            super.construct2ndStep(node, object);
        }

        private void validateKeys(MappingNode node)
        {
            List<String> keys = node.getValue().stream()
                    .map(NodeTuple::getKeyNode)
                    .filter(n -> n instanceof ScalarNode)
                    .map(n -> ((ScalarNode)n).getValue())
                    .collect(Collectors.toList());
            if (keys.stream().distinct().count() != keys.stream().count()) {
                throw new ConfigException("duplicate keys in workflow definition");
            }
        }
    }
}

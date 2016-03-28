package io.digdag.core.config;

import io.digdag.client.config.ConfigException;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.error.MarkedYAMLException;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.MappingNode;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeTuple;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.nodes.Tag;

import java.util.List;
import java.util.Map;
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
            Map<String, Long> keyCounts = node.getValue().stream()
                    .map(NodeTuple::getKeyNode)
                    .filter(n -> n instanceof ScalarNode)
                    .map(ScalarNode.class::cast)
                    .collect(Collectors.groupingBy(ScalarNode::getValue, Collectors.counting()));
            List<String> duplicatedKeys = keyCounts.entrySet().stream()
                    .filter(it -> it.getValue() > 1)
                    .map(Map.Entry<String, Long>::getKey)
                    .collect(Collectors.toList());
            if (!duplicatedKeys.isEmpty()) {
                throw new DuplicateKeyYAMLException(duplicatedKeys);
            }
        }
    }
}

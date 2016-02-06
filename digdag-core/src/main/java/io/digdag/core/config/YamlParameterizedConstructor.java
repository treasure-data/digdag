package io.digdag.core.config;

import java.io.File;
import java.io.IOException;
import com.google.common.base.Optional;
import org.yaml.snakeyaml.constructor.ConstructorException;
import org.yaml.snakeyaml.constructor.AbstractConstruct;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.nodes.ScalarNode;
import org.yaml.snakeyaml.error.Mark;
import org.yaml.snakeyaml.error.YAMLException;
import org.yaml.snakeyaml.nodes.Node;
import org.yaml.snakeyaml.nodes.NodeId;

public class YamlParameterizedConstructor
    extends SafeConstructor
{
    public YamlParameterizedConstructor()
    {
        this.yamlConstructors.put(null, new CustomTagConstructor());
    }

    private class CustomTagConstructor
            extends AbstractConstruct
    {
        public Object construct(Node node)
        {
            switch (node.getTag().getValue()) {
            case "!include":
                // TODO use implicit resolver (Resolver.addImplicitResolver) to convert "<include: path.yml" to new Tag("!include")?
                return "!include:" + java.util.UUID.randomUUID().toString();
            }
            throw new TagException(
                    "could not determine a constructor for the tag " + node.getTag(),
                    node.getStartMark());
        }

        private String validateScalar(Node node)
        {
            if (node.isTwoStepsConstruction()) {
                throw new TagException("'"+node.getTag()+"' cannot be recursive.",
                        node.getStartMark());
            }
            if (!node.getNodeId().equals(NodeId.scalar)) {
                throw new TagException("'"+node.getTag()+"' must be a string.",
                        node.getStartMark());
            }
            return ((ScalarNode) node).getValue().toString();
        }
    }

    public static class TagException
            extends ConstructorException
    {
        public TagException(String message, Mark mark)
        {
            super(null, null, message, mark);
        }
    }
}

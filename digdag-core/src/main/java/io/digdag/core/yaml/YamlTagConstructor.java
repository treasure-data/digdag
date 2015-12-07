package io.digdag.core.yaml;

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

public class YamlTagConstructor
    extends SafeConstructor
{
    public static interface Include
    {
        public Object load(String name) throws IOException;
    }

    private Optional<Include> include;

    public YamlTagConstructor()
    {
        this.include = Optional.absent();
        this.yamlConstructors.put(null, new CustomTagConstructor());
    }

    public void setInclude(Include include)
    {
        this.include = Optional.of(include);
    }

    private class CustomTagConstructor
            extends AbstractConstruct
    {
        public Object construct(Node node)
        {
            switch (node.getTag().getValue()) {
            case "!include":
                if (include.isPresent()) {
                    try {
                        return include.get().load(validateScalar(node));
                    }
                    catch (Exception ex) {
                        throw new TagException(
                                String.format("%s%nAt %s", ex.getMessage(), node.getTag()),
                                node.getStartMark());
                    }
                }
                break;
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

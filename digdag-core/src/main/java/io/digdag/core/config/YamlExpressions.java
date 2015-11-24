package io.digdag.core.config;

import java.util.Objects;
import com.google.common.base.CharMatcher;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.representer.Representer;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.tag.Tag;
import com.hubspot.jinjava.tree.TagNode;

public class YamlExpressions
{
    private static final Yaml yaml;

    static {
        // make it indent-independent literal
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);

        yaml = new Yaml(new YamlTagConstructor(), new Representer(), opts, new YamlTagResolver());
    }

    public static String dump(Object object)
    {
        String content = yaml.dump(object);
        return CharMatcher.WHITESPACE.trimFrom(content);
    }

    public static String parse(String yamlContent)
    {
        Object object = yaml.load(yamlContent);
        return dump(object);
    }

    public static class DumpTag implements Tag
    {
        @Override
        public String getName()
        {
            return "dump";
        }

        @Override
        public String interpret(TagNode tagNode, JinjavaInterpreter interpreter)
        {
            // extends PrintTag and replaced Objects.toString with dump
            return dump(interpreter.resolveELExpression(tagNode.getHelpers(), tagNode.getLineNumber()));
        }

        @Override
        public String getEndTagName()
        {
            return null;
        }
    }

    public static class ParseTag implements Tag
    {
        @Override
        public String getName()
        {
            return "parse";
        }

        @Override
        public String interpret(TagNode tagNode, JinjavaInterpreter interpreter)
        {
            String content = Objects.toString(interpreter.resolveELExpression(tagNode.getHelpers(), tagNode.getLineNumber()), "");
            return parse(content);
        }

        @Override
        public String getEndTagName()
        {
            return null;
        }
    }
}

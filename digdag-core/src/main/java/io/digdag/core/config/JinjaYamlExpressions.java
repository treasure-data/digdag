package io.digdag.core.config;

import java.util.Objects;
import java.io.IOException;
import com.google.common.base.CharMatcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.tag.Tag;
import com.hubspot.jinjava.tree.TagNode;
import com.hubspot.jinjava.interpret.IncludeTagCycleException;
import com.hubspot.jinjava.interpret.InterpretException;
import com.hubspot.jinjava.interpret.TemplateError;
import com.hubspot.jinjava.interpret.TemplateError.ErrorReason;
import com.hubspot.jinjava.interpret.TemplateError.ErrorType;
import com.hubspot.jinjava.tree.Node;
import com.hubspot.jinjava.tree.TagNode;

public class JinjaYamlExpressions
{
    private static final Logger logger = LoggerFactory.getLogger(JinjaYamlExpressions.class);

    private static final Yaml yaml;

    static {
        // make it indent-independent literal
        DumperOptions opts = new DumperOptions();
        opts.setDefaultFlowStyle(DumperOptions.FlowStyle.FLOW);

        yaml = new Yaml(new SafeConstructor(), new Representer(), opts, new YamlTagResolver());
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

    public static String load(String templateFile)
    {
        return parse(include(templateFile));
    }

    public static String include(String templateFile)
    {
        // this assumes that JinjavaInterpreter.getCurrent().getContext()
        // (root context) includes all file names added by pushIncludePath.
        JinjavaInterpreter interpreter = JinjavaInterpreter.getCurrent();

        try {
            interpreter.getContext().pushIncludePath(templateFile, -1);
        }
        catch (IncludeTagCycleException e) {
            interpreter.addError(new TemplateError(ErrorType.WARNING, ErrorReason.EXCEPTION,
                        "Include cycle detected for path: '" + templateFile + "'", null, -1, e));
            return "";
        }

        try {
            String template = interpreter.getResource(templateFile);
            Node node = interpreter.parse(template);

            interpreter.getContext().addDependency("coded_files", templateFile);

            JinjavaInterpreter child = new JinjavaInterpreter(interpreter);
            String result = child.render(node);

            interpreter.getErrors().addAll(child.getErrors());

            return result;
        }
        catch (IOException e) {
            throw new InterpretException(e.getMessage(), e);
        }
        finally {
            interpreter.getContext().popIncludePath();
        }
    }
}

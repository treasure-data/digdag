package io.digdag.core.config;

import java.util.Map;
import java.util.HashMap;
import java.util.Locale;
import java.util.List;
import java.util.ArrayList;
import java.util.Stack;
import java.util.regex.Pattern;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import com.google.common.base.Optional;
import com.google.common.collect.Lists;
import com.google.common.io.CharStreams;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.constructor.SafeConstructor;
import org.yaml.snakeyaml.representer.Representer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.hubspot.jinjava.Jinjava;
import com.hubspot.jinjava.JinjavaConfig;
import com.hubspot.jinjava.interpret.Context;
import com.hubspot.jinjava.interpret.JinjavaInterpreter;
import com.hubspot.jinjava.lib.fn.ELFunctionDefinition;
import com.hubspot.jinjava.loader.FileLocator;
import com.hubspot.jinjava.loader.ResourceNotFoundException;

public class YamlConfigLoader
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private static Pattern validIncludePattern = Pattern.compile("^(?:(?:[\\/\\:\\\\\\;])?(?![^a-zA-Z0-9_]+[\\/\\:\\\\\\;])[^\\/\\:\\\\\\;]*)+$");

    private final ObjectMapper treeObjectMapper = new ObjectMapper();
    private final ConfigFactory cf;

    // TODO set charset and timezone

    @Inject
    public YamlConfigLoader(ConfigFactory cf)
    {
        this.cf = cf;
    }

    public Config loadFile(File file)
        throws IOException
    {
        try (FileInputStream in = new FileInputStream(file)) {
            String content = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
            return loadString(content);
        }
    }

    public Config loadString(String content)
        throws IOException
    {
        // here doesn't use jackson-dataformat-yaml so that snakeyaml calls Resolver
        // and Composer. See also YamlTagResolver.
        Object object = newYaml().load(content);
        JsonNode node = treeObjectMapper.readTree(treeObjectMapper.writeValueAsString(object));
        return cf.create(validateJsonNode(node));
    }

    public Config loadParameterizedFile(File file, Config params)
        throws IOException
    {
        try (FileInputStream in = new FileInputStream(file)) {
            String content = CharStreams.toString(new InputStreamReader(in, StandardCharsets.UTF_8));
            return loadParameterizedString(content, file.getAbsoluteFile(), params);
        }
    }

    private Config loadParameterizedString(String content, File filePath, Config params)
        throws IOException
    {
        Jinjava jinjava = newJinjava(filePath);

        Map<String, Object> bindings = new HashMap<>();
        for (String key : params.getKeys()) {
            bindings.put(key, params.get(key, Object.class));
        }

        String rendered = jinjava.render(content, bindings);

        logger.debug("rendered config:\n---\n{}\n---", rendered);

        return loadString(rendered);
    }

    private static ObjectNode validateJsonNode(JsonNode node)
    {
        if (!node.isObject()) {
            throw new RuntimeJsonMappingException("Expected object to load Config but got "+node);
        }
        return (ObjectNode) node;
    }

    private Yaml newYaml()
    {
        return new Yaml(new SafeConstructor(), new Representer(), new DumperOptions(), new YamlTagResolver());
    }

    private Jinjava newJinjava(File filePath)
    {
        Jinjava jinjava = new Jinjava(
                JinjavaConfig.newBuilder()
                .withLocale(Locale.ENGLISH)
                .withCharset(StandardCharsets.UTF_8)
                //.withTimeZone(TimeZone.UTC)
                .build());

        //if (filePath.isPresent()) {
            File rootDir = filePath.getParentFile();
            jinjava.setResourceLocator((name, encoding, interpreter) -> {
                File path;
                try {
                    path = resolveIncludeFilePath(interpreter, rootDir, name);
                }
                catch (RuntimeException ex) {
                    logger.error("Error at loading another template file", ex);
                    throw ex;
                }

                try {
                    if (!path.exists() || !path.isFile()) {
                        throw new ResourceNotFoundException("Couldn't find resource: " + path);
                    }

                    return Files.toString(path, encoding);
                }
                catch (ResourceNotFoundException | RuntimeException ex) {
                    logger.error("Failed to load a file: {}", path, ex);
                    throw ex;
                }
            });
        //}
        //else {
        //    jinjava.setResourceLocator((name, encoding, interpreter) -> {
        //        throw new RuntimeException("include and load tags are not allowed in this context");
        //    });
        //}

        jinjava.getGlobalContext().registerFunction(new ELFunctionDefinition("", "dump",
                    JinjaYamlExpressions.class, "dump", Object.class));
        jinjava.getGlobalContext().registerFunction(new ELFunctionDefinition("", "parse",
                    JinjaYamlExpressions.class, "parse", String.class));
        jinjava.getGlobalContext().registerFunction(new ELFunctionDefinition("", "load",
                    JinjaYamlExpressions.class, "load", String.class));

        jinjava.getGlobalContext().registerTag(new JinjaLoadTag());
        jinjava.getGlobalContext().registerTag(new JinjaYamlExpressions.DumpTag());
        jinjava.getGlobalContext().registerTag(new JinjaYamlExpressions.ParseTag());

        return jinjava;
    }

    private static File resolveIncludeFilePath(JinjavaInterpreter interpreter, File rootDir, String fname)
    {
        if (!validIncludePattern.matcher(fname).matches()) {
            throw new RuntimeException("file name must not include .. or .: " + fname);
        }
        return getRelativeIncludePath(interpreter, rootDir, fname);
    }

    //private static File getRelativeIncludePath0(JinjavaInterpreter interpreter, File rootDir, String fname)
    //{
    //    System.out.println("loading file: "+fname);

    //    Context context = interpreter.getContext();
    //    System.out.println("current context: "+context);
    //    // this context already includes `fname`. Skip it and add it the end.
    //    Context stackTop = context.getParent();
    //    List<String> reverseStack = new ArrayList<>();

    //    // context.getParent() == null means that the context is global context.
    //    // Global context should be skpped because the child of global context
    //    // is the root context.
    //    Context parent;
    //    while ((parent = stackTop.getParent()) != null) {
    //        String subdir = getCurrentIncludingSubdir(stackTop);
    //        System.out.println("stacking: "+subdir);
    //        if (subdir != null) {
    //            reverseStack.add(subdir);
    //        }
    //        stackTop = parent;
    //    }

    //    File dir = rootDir;
    //    for (String subdir : Lists.reverse(reverseStack)) {
    //        dir = new File(dir, subdir);
    //    }

    //    return new File(dir, fname);
    //}

    //@SuppressWarnings("unchecked")
    //private static String getCurrentIncludingSubdir(Context context)
    //{
    //    Stack<String> includingFileNameStack;
    //    try {
    //        Field field = Context.class.getDeclaredField("includePathStack");
    //        field.setAccessible(true);
    //        includingFileNameStack = (Stack<String>) field.get(context);
    //    }
    //    catch (NoSuchFieldException | IllegalAccessException | ClassCastException ex) {
    //        throw new RuntimeException(ex);
    //    }
    //    System.out.println("includingFileNameStack: "+includingFileNameStack);
    //    String fname = includingFileNameStack.peek();
    //    if (fname == null) {  // this should not happen
    //        return null;
    //    }
    //    else {
    //        return new File(fname).getParent();
    //    }
    //}

    @SuppressWarnings("unchecked")
    private static File getRelativeIncludePath(JinjavaInterpreter interpreter, File rootDir, String includingFileName)
    {
        // TODO This code still has a bug because of a bug in jinja2. Context.popIncludePath wrongly pops a path
        //      from the parent Context. To fix it, popIncludePath shouldn't change parent Context, or pushIncludePath
        //      should push a path to the parent Context. This method works only with the latter fix. With the first fix,
        //      getRelativeIncludePath0 may work but then it doesn't work with JinjaYamlExpressions.include function.

        //System.out.println("including : "+includingFileName);
        //Context rootContext = getRootContext(interpreter);
        Context rootContext = interpreter.getCurrent().getContext();
        Stack<String> stack;
        try {
            Field field = Context.class.getDeclaredField("includePathStack");
            field.setAccessible(true);
            stack = (Stack<String>) field.get(rootContext);
        }
        catch (NoSuchFieldException | IllegalAccessException | ClassCastException ex) {
            throw new RuntimeException(ex);
        }

        // If the stack already includes `includingFileName`, skip it here because it will be added at the end.
        //System.out.println("original stack: "+stack);
        if (!stack.isEmpty() && includingFileName.equals(stack.peek())) {
            Stack<String> ns = new Stack<>();
            ns.addAll(stack);
            ns.pop();
            stack = ns;
        }
        //System.out.println("stack: "+stack);

        // but the stack doesn't include the root dir
        if (stack.isEmpty()) {
            return new File(rootDir, includingFileName);
        }
        else {
            File currentDir = rootDir;
            for (String fname : stack) {
                //System.out.println("fname: "+fname);
                String subdir = new File(fname).getParent();
                //System.out.println("subdir: "+subdir);
                if (subdir != null) {
                    currentDir = new File(currentDir, subdir);
                    //System.out.println("currentDir: "+currentDir);
                }
            }
            return new File(currentDir, includingFileName);
        }
    }

    //static Context getRootContext(JinjavaInterpreter interpreter)
    //{
    //    Context context = interpreter.getContext();
    //    Context lastContext;
    //    do {
    //        lastContext = context;
    //        context = context.getParent();
    //    } while (context.getParent() != null);
    //    // context is the global context. root context is lastContext.
    //    return lastContext;
    //}
}

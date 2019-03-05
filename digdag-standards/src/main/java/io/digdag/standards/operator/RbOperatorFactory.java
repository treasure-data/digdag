package io.digdag.standards.operator;

import java.util.List;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.CharStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.digdag.spi.OperatorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.util.BaseOperator;
import static io.digdag.standards.operator.ShOperatorFactory.collectEnvironmentVariables;

public class RbOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(RbOperatorFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    RbOperatorFactory.class.getResourceAsStream("/digdag/standards/rb/runner.rb"),
                    StandardCharsets.UTF_8)) {
            runnerScript = CharStreams.toString(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private final CommandExecutor exec;
    private final CommandLogger clog;
    private final ObjectMapper mapper;

    @Inject
    public RbOperatorFactory(CommandExecutor exec, CommandLogger clog,
            ObjectMapper mapper)
    {
        this.exec = exec;
        this.clog = clog;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "rb";
    }

    @Override
    public Operator newOperator(OperatorContext context)
    {
        return new RbOperator(context);
    }

    private class RbOperator
            extends BaseOperator
    {
        public RbOperator(OperatorContext context)
        {
            super(context);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("rb"))
                .merge(request.getLastStateParams());  // merge state parameters in addition to regular config

            Config data;
            try {
                data = runCode(params);
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            return TaskResult.defaultBuilder(request)
                .subtaskConfig(data.getNestedOrGetEmpty("subtask_config"))
                .exportParams(data.getNestedOrGetEmpty("export_params"))
                .storeParams(data.getNestedOrGetEmpty("store_params"))
                .build();
        }

        private Config runCode(Config params)
                throws IOException, InterruptedException
        {
            String inFile = workspace.createTempFile("digdag-rb-in-", ".tmp");
            String outFile = workspace.createTempFile("digdag-rb-out-", ".tmp");

            String script;
            List<String> args;

            if (params.has("_command")) {
                String command = params.get("_command", String.class);
                script = runnerScript;
                args = ImmutableList.of(command, inFile, outFile);
            }
            else {
                script = params.get("script", String.class);
                args = ImmutableList.of(inFile, outFile);
            }

            Optional<String> feature = params.getOptional("require", String.class);

            try (OutputStream fo = workspace.newOutputStream(inFile)) {
                mapper.writeValue(fo, ImmutableMap.of("params", params));
            }

            List<String> ruby;
            final JsonNode rubyJsonNode = params.getInternalObjectNode().get("ruby");
            if (rubyJsonNode == null) {
                ruby = ImmutableList.of("ruby");
            }
            else if (rubyJsonNode.isTextual()) {
                final String path = rubyJsonNode.asText();
                ruby = ImmutableList.of(path);
            }
            else if (rubyJsonNode.isArray()) {
                ruby = Arrays.asList(mapper.readValue(rubyJsonNode.traverse(), String[].class));
            }
            else {
                throw new ConfigException("Invalid ruby: " + rubyJsonNode.asText());
            }

            ImmutableList.Builder<String> cmdline = ImmutableList.builder();
            cmdline.addAll(ruby);
            cmdline.add("-I").add(workspace.getPath().toString());
            if (feature.isPresent()) {
                cmdline.add("-r").add(feature.get());
            }
            cmdline.add("--").add("-");  // script is fed from stdin  TODO: this doesn't work with jruby
            cmdline.addAll(args);

            logger.trace("Running rb operator: {}", cmdline.build().stream().collect(Collectors.joining(" ")));

            ProcessBuilder pb = new ProcessBuilder(cmdline.build());
            pb.directory(workspace.getPath().toFile());
            pb.redirectErrorStream(true);

            // Set up process environment according to env config. This can also refer to secrets.
            Map<String, String> env = pb.environment();
            collectEnvironmentVariables(env, context.getPrivilegedVariables());

            Process p = exec.start(workspace.getPath(), request, pb);

            // feed script to stdin
            try (Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                writer.write(script);
            }

            // read stdout to stdout
            clog.copyStdout(p, System.out);

            int ecode = p.waitFor();

            if (ecode != 0) {
                throw new RuntimeException("Ruby command failed with code " + ecode);
            }

            return mapper.readValue(workspace.getFile(outFile), Config.class);
        }
    }
}

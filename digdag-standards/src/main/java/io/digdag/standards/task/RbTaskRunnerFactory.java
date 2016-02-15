package io.digdag.standards.task;

import java.util.List;
import java.util.stream.Collectors;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import io.digdag.client.config.Config;

public class RbTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(RbTaskRunnerFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    RbTaskRunnerFactory.class.getResourceAsStream("/digdag/standards/rb/runner.rb"),
                    StandardCharsets.UTF_8)) {
            runnerScript = CharStreams.toString(reader);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private final CommandExecutor exec;
    private final ObjectMapper mapper;

    @Inject
    public RbTaskRunnerFactory(CommandExecutor exec, ObjectMapper mapper)
    {
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "rb";
    }

    @Override
    public TaskRunner newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new RbTaskRunner(archivePath, request);
    }

    private class RbTaskRunner
            extends BaseTaskRunner
    {
        public RbTaskRunner(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public Config runTask()
        {
            Config config = request.getConfig().getNestedOrGetEmpty("rb")
                .deepCopy()
                .setAll(request.getConfig());

            // merge state parameters in addition to regular config
            Config taskEnv = config.setAll(request.getLastStateParams());

            Config data;
            try {
                data = runCode(request.getConfig(), taskEnv);
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            subtaskConfig.setAll(data.getNestedOrGetEmpty("subtask_config"));
            stateParams.setAll(data.getNestedOrGetEmpty("state_params"));
            inputs.addAll(data.getListOrEmpty("inputs", Config.class));
            outputs.addAll(data.getListOrEmpty("outputs", Config.class));

            Config carryParams = data.getNestedOrGetEmpty("carry_params");
            return carryParams;
        }

        private Config runCode(Config config, Config taskEnv)
                throws IOException, InterruptedException
        {
            String inFile = archive.createTempFile("digdag-rb-in-", ".tmp");
            String outFile = archive.createTempFile("digdag-rb-out-", ".tmp");

            String script;
            List<String> args;

            if (config.has("command")) {
                String command = config.get("command", String.class);
                script = runnerScript;
                args = ImmutableList.of(command, inFile, outFile);
            }
            else {
                script = config.get("script", String.class);
                args = ImmutableList.of(inFile, outFile);
            }

            Optional<String> feature = config.getOptional("require", String.class);

            try (OutputStream fo = archive.newOutputStream(inFile)) {
                mapper.writeValue(fo, ImmutableMap.of("config", taskEnv));
            }

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                ImmutableList.Builder<String> cmdline = ImmutableList.builder();
                cmdline.add("ruby");
                cmdline.add("-I").add(archivePath.toString());
                if (feature.isPresent()) {
                    cmdline.add("-r").add(feature.get());
                }
                cmdline.add("--").add("-");  // script is fed from stdin  TODO: this doesn't work with jruby
                cmdline.addAll(args);

                ProcessBuilder pb = new ProcessBuilder(cmdline.build());
                pb.redirectErrorStream(true);
                Process p = exec.start(archivePath, request, pb);

                // feed script to stdin
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                    writer.write(script);
                }

                // read stdout to buffer
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, buffer);
                }

                ecode = p.waitFor();
                message = buffer.toString();
            }

            //logger.info("Ruby message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Ruby command failed: "+message);
            }

            return mapper.readValue(archive.getFile(outFile), Config.class);
        }
    }
}

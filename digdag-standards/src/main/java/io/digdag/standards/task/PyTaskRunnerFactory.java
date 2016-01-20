package io.digdag.standards.task;

import java.util.List;
import java.util.stream.Collectors;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.Inject;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;

public class PyTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(PyTaskRunnerFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    PyTaskRunnerFactory.class.getResourceAsStream("/digdag/standards/py/runner.py"),
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
    public PyTaskRunnerFactory(CommandExecutor exec, ObjectMapper mapper)
    {
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    @Override
    public TaskRunner newTaskExecutor(TaskRequest request)
    {
        return new PyTaskRunner(request);
    }

    private class PyTaskRunner
            extends BaseTaskRunner
    {
        public PyTaskRunner(TaskRequest request)
        {
            super(request);
        }

        @Override
        public Config runTask()
        {
            Config taskEnv = request.getConfig().deepCopy()
                .setAll(request.getLastStateParams());
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
            File inFile = File.createTempFile("digdag-py-in-", ".tmp");  // TODO use TempFileAllocator
            File outFile = File.createTempFile("digdag-py-out-", ".tmp");  // TODO use TempFileAllocator

            String script;
            List<String> args;

            if (config.has("command")) {
                String command = config.get("command", String.class);
                script = runnerScript;
                args = ImmutableList.of(command, inFile.toString(), outFile.toString());
            }
            else {
                script = config.get("script", String.class);
                args = ImmutableList.of(inFile.toString(), outFile.toString());
            }

            try (FileOutputStream fo = new FileOutputStream(inFile)) {
                mapper.writeValue(fo, ImmutableMap.of("config", taskEnv));
            }

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                List<String> cmdline = ImmutableList.<String>builder()
                    .add("python").add("-")  // script is fed from stdin
                    .addAll(args)
                    .build();
                ProcessBuilder pb = new ProcessBuilder(cmdline);
                pb.redirectErrorStream(true);
                Process p = exec.start(request, pb);

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

            //logger.info("Python message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Python command failed: "+message);
            }

            return mapper.readValue(outFile, Config.class);
        }
    }
}

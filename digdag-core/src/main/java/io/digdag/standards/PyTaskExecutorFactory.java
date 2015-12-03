package io.digdag.standards;

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
import io.digdag.core.spi.CommandExecutor;
import io.digdag.core.spi.TaskRequest;
import io.digdag.core.spi.TaskExecutor;
import io.digdag.core.spi.TaskExecutorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.config.Config;

public class PyTaskExecutorFactory
        implements TaskExecutorFactory
{
    private static Logger logger = LoggerFactory.getLogger(PyTaskExecutorFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    PyTaskExecutorFactory.class.getResourceAsStream("/digdag/standards/py/runner.py"),
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
    public PyTaskExecutorFactory(CommandExecutor exec, ObjectMapper mapper)
    {
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    public TaskExecutor newTaskExecutor(TaskRequest request)
    {
        return new PyTaskExecutor(request);
    }

    private class PyTaskExecutor
            extends BaseTaskExecutor
    {
        public PyTaskExecutor(TaskRequest request)
        {
            super(request);
        }

        @Override
        public Config runTask()
        {
            Config taskEnv = request.getParams().deepCopy()
                .setAll(request.getConfig())
                .setAll(request.getParams())
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

            // TODO distinguish carry params from export params?
            Config carryParams = data.getNestedOrGetEmpty("carry_params");
            Config exportParams = data.getNestedOrGetEmpty("export_params");
            return exportParams.setAll(carryParams);
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
                logger.info("py>: {}", command);
            }
            else {
                script = config.get("script", String.class);
                args = ImmutableList.of(inFile.toString(), outFile.toString());
                logger.info("py>: script");
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

package io.digdag.standards.operator;

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
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.client.config.Config;

public class PyOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(PyOperatorFactory.class);

    private final String runnerScript;

    {
        try (InputStreamReader reader = new InputStreamReader(
                    PyOperatorFactory.class.getResourceAsStream("/digdag/standards/py/runner.py"),
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
    public PyOperatorFactory(CommandExecutor exec, ObjectMapper mapper)
    {
        this.exec = exec;
        this.mapper = mapper;
    }

    public String getType()
    {
        return "py";
    }

    @Override
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new PyOperator(archivePath, request);
    }

    private class PyOperator
            extends BaseOperator
    {
        public PyOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config config = request.getConfig().getNestedOrGetEmpty("py")
                .deepCopy()
                .setAll(request.getConfig());

            // merge state parameters in addition to regular config
            Config params = config.setAll(request.getLastStateParams());

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
            String inFile = archive.createTempFile("digdag-py-in-", ".tmp");
            String outFile = archive.createTempFile("digdag-py-out-", ".tmp");

            String script;
            List<String> args;

            if (params.has("command")) {
                String command = params.get("command", String.class);
                script = runnerScript;
                args = ImmutableList.of(command, inFile, outFile);
            }
            else {
                script = params.get("script", String.class);
                args = ImmutableList.of(inFile, outFile);
            }

            try (OutputStream fo = archive.newOutputStream(inFile)) {
                mapper.writeValue(fo, ImmutableMap.of("params", params));
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

            //logger.info("Python message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Python command failed: "+message);
            }

            return mapper.readValue(archive.getFile(outFile), Config.class);
        }
    }
}

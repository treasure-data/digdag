package io.digdag.standards.operator;

import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.google.common.io.ByteStreams;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import static java.nio.charset.StandardCharsets.UTF_8;

public class EmbulkOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(EmbulkOperatorFactory.class);

    private final CommandExecutor exec;
    private final TemplateEngine templateEngine;
    private final ObjectMapper mapper;
    private final YAMLFactory yaml;

    @Inject
    public EmbulkOperatorFactory(CommandExecutor exec, TemplateEngine templateEngine, ObjectMapper mapper)
    {
        this.exec = exec;
        this.templateEngine = templateEngine;
        this.mapper = mapper;
        this.yaml = new YAMLFactory()
            .configure(YAMLGenerator.Feature.WRITE_DOC_START_MARKER, false);
    }

    public String getType()
    {
        return "embulk";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new EmbulkOperator(workspacePath, request);
    }

    private class EmbulkOperator
            extends BaseOperator
    {
        public EmbulkOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config params = request.getConfig().mergeDefault(
                    request.getConfig().getNestedOrGetEmpty("embulk"));

            String tempFile;
            try {
                tempFile = workspace.createTempFile("digdag-embulk-", ".tmp.yml");

                if (params.has("_command")) {
                    String command = params.get("_command", String.class);
                    String data = templateEngine.templateFile(workspacePath, command, UTF_8, params);
                    Files.write(workspace.getPath(tempFile), data.getBytes(UTF_8));
                }
                else {
                    Config embulkConfig = params.getNested("config");
                    try (YAMLGenerator out = yaml.createGenerator(workspace.newOutputStream(tempFile), JsonEncoding.UTF8)) {
                        mapper.writeValue(out, embulkConfig);
                    }
                }
            }
            catch (IOException | TemplateException ex) {
                throw Throwables.propagate(ex);
            }

            ProcessBuilder pb = new ProcessBuilder("embulk", "run", tempFile);

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                Process p = exec.start(workspacePath, request, pb);
                p.getOutputStream().close();
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, buffer);
                }
                ecode = p.waitFor();
                message = buffer.toString();
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            //logger.info("Shell command message ===\n{}", message);  // TODO include task name
            System.out.println(message);
            if (ecode != 0) {
                throw new RuntimeException("Command failed: "+message);
            }

            return TaskResult.empty(request);
        }
    }
}

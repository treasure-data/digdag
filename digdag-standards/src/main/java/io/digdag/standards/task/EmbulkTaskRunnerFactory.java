package io.digdag.standards.task;

import java.util.List;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharStreams;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TemplateEngine;
import io.digdag.spi.TemplateException;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskRunner;
import io.digdag.spi.TaskRunnerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;
import static java.nio.charset.StandardCharsets.UTF_8;

public class EmbulkTaskRunnerFactory
        implements TaskRunnerFactory
{
    private static Logger logger = LoggerFactory.getLogger(EmbulkTaskRunnerFactory.class);

    private final CommandExecutor exec;
    private final TemplateEngine templateEngine;
    private final ObjectMapper mapper;
    private final YAMLFactory yaml;

    @Inject
    public EmbulkTaskRunnerFactory(CommandExecutor exec, TemplateEngine templateEngine, ObjectMapper mapper)
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
    public TaskRunner newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new EmbulkTaskRunner(archivePath, request);
    }

    private class EmbulkTaskRunner
            extends BaseTaskRunner
    {
        public EmbulkTaskRunner(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public Config runTask()
        {
            Config config = request.getConfig().getNestedOrGetEmpty("embulk")
                .deepCopy()
                .setAll(request.getConfig());

            File tempFile;
            try {
                tempFile = File.createTempFile("digdag-embulk-", ".tmp.yml");  // TODO use TempFileAllocator

                if (config.has("command")) {
                    String command = config.get("command", String.class);
                    String data = templateEngine.templateFile(archivePath, command, UTF_8, config);
                    Files.write(tempFile.toPath(), data.getBytes(UTF_8));
                }
                else {
                    Config embulkConfig = config.getNested("config");
                    try (YAMLGenerator out = yaml.createGenerator(new FileOutputStream(tempFile), JsonEncoding.UTF8)) {
                        mapper.writeValue(out, embulkConfig);
                    }
                }
            }
            catch (IOException | TemplateException ex) {
                throw Throwables.propagate(ex);
            }

            ProcessBuilder pb = new ProcessBuilder("embulk", "run", tempFile.toString());

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                Process p = exec.start(archivePath, request, pb);
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

            return request.getConfig().getFactory().create();
        }
    }
}

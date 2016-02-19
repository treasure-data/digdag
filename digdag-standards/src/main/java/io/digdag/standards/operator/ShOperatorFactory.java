package io.digdag.standards.operator;

import java.util.Map;
import java.util.regex.Pattern;
import java.io.File;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.client.config.Config;

public class ShOperatorFactory
        implements OperatorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ShOperatorFactory.class);

    private static Pattern VALID_ENV_KEY = Pattern.compile("[a-zA-Z_]+");

    private final CommandExecutor exec;

    @Inject
    public ShOperatorFactory(CommandExecutor exec)
    {
        this.exec = exec;
    }

    public String getType()
    {
        return "sh";
    }

    @Override
    public Operator newTaskExecutor(Path archivePath, TaskRequest request)
    {
        return new ShOperator(archivePath, request);
    }

    private class ShOperator
            extends BaseOperator
    {
        public ShOperator(Path archivePath, TaskRequest request)
        {
            super(archivePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            String command = request.getConfig().get("command", String.class);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

            final Map<String, String> env = pb.environment();
            request.getConfig().getKeys()
                .forEach(key -> {
                    if (isValidEnvKey(key)) {
                        JsonNode value = request.getConfig().get(key, JsonNode.class);
                        String string;
                        if (value.isTextual()) {
                            string = value.textValue();
                        }
                        else {
                            string = value.toString();
                        }
                        env.put(key, string);
                    }
                    else {
                        logger.trace("Ignoring invalid env var key: {}", key);
                    }
                });

            // add archive path to the end of $PATH so that bin/cmd works without ./ at the beginning
            String pathEnv = System.getenv("PATH");
            if (pathEnv == null) {
                pathEnv = archivePath.toAbsolutePath().toString();
            }
            else {
                pathEnv = pathEnv + File.pathSeparator + archivePath.toAbsolutePath().toString();
            }

            pb.redirectErrorStream(true);

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

            return TaskResult.empty(request.getConfig().getFactory());
        }
    }

    private static boolean isValidEnvKey(String key)
    {
        return VALID_ENV_KEY.matcher(key).matches();
    }
}

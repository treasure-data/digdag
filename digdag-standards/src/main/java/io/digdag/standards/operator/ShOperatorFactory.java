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
import io.digdag.spi.CommandLogger;
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

    private static Pattern VALID_ENV_KEY = Pattern.compile("[a-zA-Z_][a-zA-Z_0-9]*");

    private final CommandExecutor exec;
    private final CommandLogger clog;

    @Inject
    public ShOperatorFactory(CommandExecutor exec, CommandLogger clog)
    {
        this.exec = exec;
        this.clog = clog;
    }

    public String getType()
    {
        return "sh";
    }

    @Override
    public Operator newTaskExecutor(Path workspacePath, TaskRequest request)
    {
        return new ShOperator(workspacePath, request);
    }

    private class ShOperator
            extends BaseOperator
    {
        public ShOperator(Path workspacePath, TaskRequest request)
        {
            super(workspacePath, request);
        }

        @Override
        public TaskResult runTask()
        {
            Config params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("sh"));

            String command = params.get("_command", String.class);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

            final Map<String, String> env = pb.environment();
            params.getKeys()
                .forEach(key -> {
                    if (isValidEnvKey(key)) {
                        JsonNode value = params.get(key, JsonNode.class);
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

            // add workspace path to the end of $PATH so that bin/cmd works without ./ at the beginning
            String pathEnv = System.getenv("PATH");
            if (pathEnv == null) {
                pathEnv = workspacePath.toAbsolutePath().toString();
            }
            else {
                pathEnv = pathEnv + File.pathSeparator + workspacePath.toAbsolutePath().toString();
            }

            pb.redirectErrorStream(true);

            int ecode;
            try {
                Process p = exec.start(workspacePath, request, pb);
                p.getOutputStream().close();

                // copy stdout to System.out and logger
                clog.copyStdout(p, System.out);

                ecode = p.waitFor();
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            if (ecode != 0) {
                throw new RuntimeException("Command failed with code " + ecode);
            }

            return TaskResult.empty(request);
        }
    }

    private static boolean isValidEnvKey(String key)
    {
        return VALID_ENV_KEY.matcher(key).matches();
    }
}

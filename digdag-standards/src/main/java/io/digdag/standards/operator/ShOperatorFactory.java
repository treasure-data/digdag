package io.digdag.standards.operator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeType;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.spi.CommandExecutor;
import io.digdag.spi.CommandLogger;
import io.digdag.spi.Operator;
import io.digdag.spi.OperatorFactory;
import io.digdag.spi.TaskExecutionContext;
import io.digdag.spi.TaskRequest;
import io.digdag.spi.TaskResult;
import io.digdag.util.BaseOperator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

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
    public Operator newOperator(Path projectPath, TaskRequest request)
    {
        return new ShOperator(projectPath, request);
    }

    private class ShOperator
            extends BaseOperator
    {
        public ShOperator(Path projectPath, TaskRequest request)
        {
            super(projectPath, request);
        }

        @Override
        public List<String> secretSelectors()
        {
            // Return the secret keys referred to in the env config
            Set<String> selectors = new HashSet<>();
            Config envConfig = this.request.getConfig().getNestedOrGetEmpty("_env");
            for (String name : envConfig.getKeys()) {
                if (!VALID_ENV_KEY.matcher(name).matches()) {
                    throw new ConfigException("Invalid _env");
                }
                JsonNode value = envConfig.get(name, JsonNode.class);
                switch(value.getNodeType()) {
                    case OBJECT:
                        ObjectNode ref = (ObjectNode) value;
                        JsonNode secret = ref.get("secret");
                        if (ref.size() != 1 || secret == null || !secret.isTextual()) {
                            throw new ConfigException("Invalid _env");
                        }
                        String secretKey = secret.textValue();
                        selectors.add(secretKey);
                        break;
                    case STRING:
                        break;
                    default:
                        throw new ConfigException("Invalid _env");
                }
            }
            return ImmutableList.copyOf(selectors);
        }

        @Override
        public TaskResult runTask(TaskExecutionContext ctx)
        {
            Config params = request.getConfig()
                .mergeDefault(request.getConfig().getNestedOrGetEmpty("sh"));

            List<String> shell = params.getListOrEmpty("shell", String.class);
            if (shell.isEmpty()) {
                shell = ImmutableList.of("/bin/sh");
            }
            String command = params.get("_command", String.class);

            ProcessBuilder pb = new ProcessBuilder(shell);
            pb.directory(workspace.getPath().toFile());

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

            // Set up process environment according to env config. This can also refer to secrets.
            Config envConfig = this.request.getConfig().getNestedOrGetEmpty("_env");
            for (String name : envConfig.getKeys()) {
                if (!VALID_ENV_KEY.matcher(name).matches()) {
                    throw new ConfigException("Invalid _env");
                }
                JsonNode value = envConfig.get(name, JsonNode.class);
                switch(value.getNodeType()) {
                    case OBJECT:
                        ObjectNode ref = (ObjectNode) value;
                        JsonNode secret = ref.get("secret");
                        if (ref.size() != 1 || secret == null || !secret.isTextual()) {
                            throw new ConfigException("Invalid _env");
                        }
                        String secretKey = secret.textValue();
                        String secretValue = ctx.secrets().getSecret(secretKey);
                        env.put(name, secretValue);
                        break;
                    case STRING:
                        env.put(name, value.textValue());
                        break;
                    default:
                        throw new ConfigException("Invalid _env");
                }
            }

            // add workspace path to the end of $PATH so that bin/cmd works without ./ at the beginning
            String pathEnv = System.getenv("PATH");
            if (pathEnv == null) {
                pathEnv = workspace.getPath().toString();
            }
            else {
                pathEnv = pathEnv + File.pathSeparator + workspace.getPath().toAbsolutePath().toString();
            }

            pb.redirectErrorStream(true);

            int ecode;
            try {
                Process p = exec.start(workspace.getPath(), request, pb);

                // feed command to stdin
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(p.getOutputStream()))) {
                    writer.write(command);
                }

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

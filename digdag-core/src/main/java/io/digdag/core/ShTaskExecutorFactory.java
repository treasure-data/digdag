package io.digdag.core;

import java.util.Map;
import java.io.InputStream;
import java.io.IOException;
import java.io.ByteArrayOutputStream;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.core.config.Config;

public class ShTaskExecutorFactory
        implements TaskExecutorFactory
{
    private static Logger logger = LoggerFactory.getLogger(ShTaskExecutorFactory.class);

    @Inject
    public ShTaskExecutorFactory()
    {
    }

    public String getType()
    {
        return "sh";
    }

    public TaskExecutor newTaskExecutor(Config config, Config params, Config state)
    {
        return new ShTaskExecutor(config, params, state);
    }

    private class ShTaskExecutor
            extends BaseTaskExecutor
    {
        public ShTaskExecutor(Config config, Config params, Config state)
        {
            super(config, params, state);
        }

        @Override
        public Config runTask(Config config, final Config params)
        {
            String command = config.get("command", String.class);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

            logger.info("sh>: {}", command);

            final Map<String, String> env = pb.environment();
            params.getKeys()
                .forEach(key -> {
                    JsonNode value = params.get(key, JsonNode.class);
                    String string;
                    if (value.isTextual()) {
                        string = value.textValue();
                    }
                    else {
                        string = value.toString();
                    }
                    env.put(key, string);
                });

            pb.redirectErrorStream(true);

            int ecode;
            String message;
            try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                Process p = pb.start();
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

            return params.getFactory().create();
        }
    }
}

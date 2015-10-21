package io.digdag.core;

import java.util.Map;
import java.io.InputStream;
import java.io.IOException;
import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.io.ByteStreams;

public class ShTaskExecutorFactory
        implements TaskExecutorFactory
{
    @Inject
    public ShTaskExecutorFactory()
    {
    }

    public String getType()
    {
        return "sh";
    }

    public TaskExecutor newTaskExecutor(ConfigSource config, ConfigSource params, ConfigSource state)
    {
        return new ShTaskExecutor(config, params, state);
    }

    private class ShTaskExecutor
            extends BaseTaskExecutor
    {
        public ShTaskExecutor(ConfigSource config, ConfigSource params, ConfigSource state)
        {
            super(config, params, state);
        }

        @Override
        public ConfigSource runTask(ConfigSource config, ConfigSource params)
        {
            String command = config.get("command", String.class);
            ProcessBuilder pb = new ProcessBuilder("/bin/sh", "-c", command);

            System.out.println("running command: "+command);
            final Map<String, String> env = pb.environment();
            params.getEntries()
                .forEach(pair -> {
                    env.put(pair.getKey(), pair.getValue().toString());
                });

            pb.redirectErrorStream(true);

            int ecode;
            try {
                Process p = pb.start();
                p.getOutputStream().close();
                try (InputStream stdout = p.getInputStream()) {
                    ByteStreams.copy(stdout, System.out);
                }
                ecode = p.waitFor();
            }
            catch (IOException | InterruptedException ex) {
                throw Throwables.propagate(ex);
            }

            if (ecode != 0) {
                throw new RuntimeException("Command failed");
            }

            return params.getFactory().create();
        }
    }
}

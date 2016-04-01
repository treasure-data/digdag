package io.digdag.core.agent;

import com.google.inject.Inject;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import io.digdag.core.log.TaskContextLogging;
import io.digdag.core.log.TaskLogger;
import io.digdag.spi.CommandLogger;
import static java.nio.charset.StandardCharsets.UTF_8;

public class TaskContextCommandLogger
    implements CommandLogger
{
    @Inject
    public TaskContextCommandLogger()
    { }

    @Override
    public void copy(InputStream in, OutputStream copy)
        throws IOException
    {
        TaskLogger logger = TaskContextLogging.getContext().getLogger();

        byte[] buffer = new byte[16*1024];
        while (true) {
            int r = in.read(buffer);
            if (r < 0) {
                break;
            }

            logger.log(buffer, 0, r);
            copy.write(buffer, 0, r);
        }
    }
}

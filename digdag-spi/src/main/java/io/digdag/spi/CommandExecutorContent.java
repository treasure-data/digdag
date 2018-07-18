package io.digdag.spi;

import java.io.IOException;
import java.io.InputStream;

public interface CommandExecutorContent
{
    long getLength();

    InputStream newInputStream()
            throws IOException;
}

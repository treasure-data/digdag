package io.digdag.spi;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public interface CommandLogger
{
    default void copyStdout(Process p, OutputStream copy)
        throws IOException
    {
        try (InputStream in = p.getInputStream()) {
            copy(in, copy);
        }
    }

    void copy(InputStream in, OutputStream copy)
        throws IOException;
}

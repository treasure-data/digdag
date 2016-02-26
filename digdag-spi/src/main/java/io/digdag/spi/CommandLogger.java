package io.digdag.spi;

import java.io.PrintStream;
import java.io.IOException;
import java.io.InputStream;

public interface CommandLogger
{
    default void copyStdout(Process p, PrintStream copy)
        throws IOException
    {
        try (InputStream in = p.getInputStream()) {
            copy(in, copy);
        }
    }

    void copy(InputStream in, PrintStream copy)
        throws IOException;
}

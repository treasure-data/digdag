package io.digdag.spi;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Path;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandStatus
{
    boolean isFinished();

    int statusCode();

    Path ioDirectory();

    ObjectNode json();
}
package io.digdag.spi;

import java.nio.file.Path;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandExecutorContext
{
    Path localProjectPath();

    Path workspacePath();

    TaskRequest taskRequest();
}

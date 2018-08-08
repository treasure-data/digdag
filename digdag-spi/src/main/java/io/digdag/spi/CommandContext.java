package io.digdag.spi;

import java.nio.file.Path;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandContext
{
    Path getLocalProjectPath();

    TaskRequest getTaskRequest();

    static ImmutableCommandContext.Builder builder()
    {
        return ImmutableCommandContext.builder();
    }
}

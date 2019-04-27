package io.digdag.spi;

import java.nio.file.Path;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandContext
{
    /**
     * Returns an absolute path of project directory.
     *
     * @return
     */
    Path getLocalProjectPath();

    /**
     * Returns task request.
     *
     * @return
     */
    TaskRequest getTaskRequest();

    static ImmutableCommandContext.Builder builder()
    {
        return ImmutableCommandContext.builder();
    }
}

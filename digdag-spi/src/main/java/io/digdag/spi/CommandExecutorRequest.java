package io.digdag.spi;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandExecutorRequest
{
    /**
     * Returns a relative path of working dir from CommandExecutorContent.getLocalProjectPath().
     *
     * @return
     */
    Path getWorkingDirectory();

    Map<String, String> getEnvironments();

    List<String> getCommand();


    /**
     * Returns a dir where scripts files are located. It must be a relative path from
     * CommandExecutorContent.getLocalProjectPath().
     *
     * @return
     */
    Path getIoDirectory();  // => .digdag/tmp/random/

    static ImmutableCommandExecutorRequest.Builder builder()
    {
        return ImmutableCommandExecutorRequest.builder();
    }
}

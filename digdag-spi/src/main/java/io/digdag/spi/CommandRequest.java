package io.digdag.spi;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandRequest
{
    /**
     * Returns a relative path of working dir from CommandContent.getLocalProjectPath().
     *
     * @return
     */
    Path getWorkingDirectory();

    Map<String, String> getEnvironments();

    List<String> getCommandLine();


    /**
     * Returns a dir where scripts files are located. It must be a relative path from
     * CommandContent.getLocalProjectPath().
     *
     * @return
     */
    Path getIoDirectory();  // => .digdag/tmp/random/

    static ImmutableCommandRequest.Builder builder()
    {
        return ImmutableCommandRequest.builder();
    }
}

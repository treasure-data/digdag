package io.digdag.spi;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.immutables.value.Value;

@Value.Immutable
public interface CommandRequest
{
    /**
     * Returns a relative path of working dir from CommandContext.getLocalProjectPath().
     *
     * @return
     */
    Path getWorkingDirectory();

    /**
     * Returns pairs of keys and values of environment variables as Map object.
     *
     * @return
     */
    Map<String, String> getEnvironments();

    /**
     * Returns a command line as List object.
     *
     * @return
     */
    List<String> getCommandLine();


    /**
     * Returns a relative path of "IO" directory from CommandContext.getLocalProjectPath().
     *
     * "IO" directory is a temporal directory that is used for passing/receiving input/output
     * config parameters to/from command runner scripts/operators. Operators serialize input config
     * parameters in JSON format and should write them to input files under the "IO" directory.
     * To receive input config parameters from operators, command runner scripts can read the file and
     * deserialize because command runner scripts should know the "IO" directory. On the other
     * hand When commands finish by the command runner scripts, to send output config parameters to
     * operators, command runner scripts serialize and should write them to output files under
     * the "IO" directory. Operators can extract the output parameters from the files.
     *
     * Command executors don't need to care of actual input/output files. CommandExecutor is
     * executed by operators. And it executes commands with command runner scripts. Instead pairs of
     * operators and command runner scripts are developed by same persons but, CommandExecutors may be
     * developed by different persons from ones who develop operators and command runner scripts.
     * Actual input/output files are not shared to CommandExecutor from operators. The "IO" directory
     * only should be shared.
     *
     * Please see PyOperatorFactory more details.
     *
     * @return
     */
    Path getIoDirectory();  // => .digdag/tmp/random/

    static ImmutableCommandRequest.Builder builder()
    {
        return ImmutableCommandRequest.builder();
    }
}

package io.digdag.server.rs;

import com.google.common.collect.ImmutableList;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.api.RestCompareResult;
import io.digdag.client.config.Config;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import static java.nio.charset.StandardCharsets.UTF_8;

public class DiffRunner
{
    private final String diffCommand;

    @Inject
    public DiffRunner(Config systemConfig)
    {
        this.diffCommand = systemConfig.getOptional("api.compare.diff_command", String.class).or("diff");
    }

    public RestCompareResult diff(
            Path fromPath, String fromName,
            Path toPath, String toName)
        throws IOException
    {
        // -u: unified format
        // -c: context format
        List<String> cmdline = ImmutableList.<String>builder()
            .add(diffCommand)
            .add("-N")
            .add("-r")
            .add("-u")
            .add(fromPath.toAbsolutePath().toString())
            .add(toPath.toAbsolutePath().toString())
            .build();
        ProcessBuilder pb = new ProcessBuilder(cmdline);
        pb.redirectOutput(ProcessBuilder.Redirect.PIPE);
        pb.redirectErrorStream(false);

        try {
            Process p = pb.start();
            try (InputStream in = p.getInputStream()) {
                return parseDiff(in);
            }
            finally {
                p.waitFor();
            }
        }
        catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(ex);
        }
    }

    private RestCompareResult parseDiff(InputStream in)
            throws IOException
    {
        String raw = new String(ByteStreams.toByteArray(in), UTF_8);
        return RestCompareResult.builder().raw(raw).build();
    }
}

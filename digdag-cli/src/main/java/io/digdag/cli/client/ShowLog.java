package io.digdag.cli.client;

import java.util.List;
import java.util.zip.GZIPInputStream;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.EOFException;
import com.google.common.base.Optional;
import io.digdag.cli.SystemExitException;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestTask;
import io.digdag.client.api.RestLogFileHandle;
import static io.digdag.cli.Main.systemExit;
import static java.nio.charset.StandardCharsets.UTF_8;

public class ShowLog
    extends ClientCommand
{
    @Override
    public void mainWithClientException()
        throws Exception
    {
        switch (args.size()) {
        case 1:
            showLogs(parseLongOrUsage(args.get(0)), Optional.absent());
            break;
        case 2:
            showLogs(parseLongOrUsage(args.get(0)), Optional.of(args.get(1)));
            break;
        default:
            throw usage(null);
        }
    }

    public SystemExitException usage(String error)
    {
        System.err.println("Usage: digdag log <session-id> [+task name prefix]");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showLogs(long sessionId, Optional<String> taskName)
        throws Exception
    {
        DigdagClient client = buildClient();

        List<RestLogFileHandle> handles;
        if (taskName.isPresent()) {
            handles = client.getLogFileHandlesOfTask(sessionId, taskName.get());
        }
        else {
            handles = client.getLogFileHandlesOfSession(sessionId);
        }

        for (RestLogFileHandle handle : handles) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(client.getLogFile(sessionId, handle.getFileName())), UTF_8))) {
                System.out.println(reader.readLine());
            }
            catch (EOFException ex) {
                // OK to ignore Unexpected end of ZLIB input stream
            }
        }
    }
}

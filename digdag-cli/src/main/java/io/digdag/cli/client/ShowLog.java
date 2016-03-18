package io.digdag.cli.client;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.time.Instant;
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
        System.err.println("Usage: digdag log <attempt-id> [+task name prefix]");
        System.err.println("  Options:");
        ClientCommand.showCommonOptions();
        return systemExit(error);
    }

    public void showLogs(long attemptId, Optional<String> taskName)
        throws Exception
    {
        DigdagClient client = buildClient();

        List<RestLogFileHandle> handles;
        if (taskName.isPresent()) {
            handles = client.getLogFileHandlesOfTask(attemptId, taskName.get());
        }
        else {
            handles = client.getLogFileHandlesOfAttempt(attemptId);
        }

        for (Map.Entry<String, List<RestLogFileHandle>> pair : sortHandles(handles).entrySet()) {
            for (RestLogFileHandle handle : pair.getValue()) {
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(client.getLogFile(attemptId, handle.getFileName())), UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        System.out.println(line);
                    }
                }
                catch (EOFException ex) {
                    // OK to ignore Unexpected end of ZLIB input stream
                }
            }
        }
    }

    private LinkedHashMap<String, List<RestLogFileHandle>> sortHandles(List<RestLogFileHandle> handles)
    {
        // group by taskName
        Map<String, List<RestLogFileHandle>> group = new HashMap<>();
        for (RestLogFileHandle handle : handles) {
            List<RestLogFileHandle> list = group.get(handle.getTaskName());
            if (list == null) {
                list = new ArrayList<>();
                group.put(handle.getTaskName(), list);
            }
            list.add(handle);
        }

        // sort by min(fileTime)
        List<List<RestLogFileHandle>> sorted = new ArrayList<>(group.values());
        Collections.sort(sorted, new Comparator<List<RestLogFileHandle>>() {
            public int compare(List<RestLogFileHandle> o1, List<RestLogFileHandle> o2)
            {
                return getMinTime(o1).compareTo(getMinTime(o2));
            }

            public boolean equals(Object o)
            {
                return o == this;
            }
        });

        LinkedHashMap<String, List<RestLogFileHandle>> map = new LinkedHashMap<>();
        for (List<RestLogFileHandle> list : sorted) {
            map.put(list.get(0).getTaskName(), list);
        }

        return map;
    }

    private static Instant getMinTime(List<RestLogFileHandle> list)
    {
        Instant min = null;
        for (RestLogFileHandle handle : list) {
            if (min == null || min.isAfter(handle.getFileTime())) {
                min = handle.getFileTime();
            }
        }
        return min;
    }
}

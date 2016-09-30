package io.digdag.cli.client;

import java.io.PrintStream;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Comparator;
import java.util.Collections;
import java.util.zip.GZIPInputStream;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.time.Instant;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.EOFException;
import com.google.common.base.Optional;
import io.digdag.client.DigdagClient;
import io.digdag.client.api.RestLogFileHandle;
import io.digdag.core.log.LogLevel;
import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;

class TaskLogWatcher
{
    // 2016-04-06 17:07:16 -0700 [INFO] ...
    private static final Pattern LEVEL_PATTERN = Pattern.compile("^[0-9\\:\\ \\-\\+\\.]*\\[([A-Za-z]+)\\]");

    private final DigdagClient client;
    private final long attemptId;
    private final Map<String, TaskLogState> stateMap;
    private final LogLevel levelFilter;
    private final PrintStream out;

    TaskLogWatcher(DigdagClient client, long attemptId, LogLevel levelFilterOrNull, PrintStream out)
    {
        this.client = client;
        this.attemptId = attemptId;
        this.levelFilter = levelFilterOrNull;
        this.out = out;
        this.stateMap = new HashMap<>();
    }

    boolean update(List<RestLogFileHandle> handles)
        throws IOException
    {
        boolean updatedAtLeastOne = false;

        for (Map.Entry<String, List<RestLogFileHandle>> pair : sortHandles(handles).entrySet()) {
            TaskLogState state = stateMap.get(pair.getKey());
            if (state == null) {
                state = new TaskLogState();
                stateMap.put(pair.getKey(), state);
            }

            boolean updated = state.update(pair.getValue());
            if (updated) {
                updatedAtLeastOne = true;
            }
        }

        return updatedAtLeastOne;
    }

    private class TaskLogState
    {
        private RestLogFileHandle lastFile = null;
        private int lastLineCount = 0;
        private boolean lastLineFiltered = false;

        boolean update(List<RestLogFileHandle> sortedHandles)
            throws IOException
        {
            int i = 0;
            boolean updated = false;

            if (lastFile != null) {
                // skip unnecessary files...
                for (; i < sortedHandles.size(); i++) {
                    RestLogFileHandle handle = sortedHandles.get(i);
                    if (lastFile.getFileTime().isAfter(handle.getFileTime())) {
                        continue;  // this file is already shown before. skip it
                    }
                    else if (lastFile.getFileName().equals(handle.getFileName())) {
                        // showing the last file if its size is grown
                        if (handle.getFileSize() > lastFile.getFileSize()) {
                            showFileAndUpdate(handle, lastLineCount, lastLineFiltered);
                            updated = true;
                        }
                        // following files are all new files
                        i++;
                        break;
                    }
                    else {
                        // following files are all new files
                        break;
                    }
                }
            }

            // show all following files
            for (; i < sortedHandles.size(); i++) {
                RestLogFileHandle handle = sortedHandles.get(i);
                showFileAndUpdate(handle, 0, false);
                updated = true;
            }

            return updated;
        }

        private void showFileAndUpdate(RestLogFileHandle handle, int lineOffset, boolean lastLineFiltered)
            throws IOException
        {
            int lines = 0;

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new GZIPInputStream(client.getLogFile(ClientCommand.id(attemptId), handle)), UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lines++;
                    if (lineOffset > 0) {
                        lineOffset--;
                        continue;
                    }
                    lastLineFiltered = showOrFilterLine(line, lastLineFiltered);
                }
            }
            catch (EOFException ex) {
                // OK to ignore Unexpected end of ZLIB input stream
            }

            this.lastFile = handle;
            this.lastLineCount = lines;
            this.lastLineFiltered = false;
        }
    }

    private boolean showOrFilterLine(String line, boolean lastLineFiltered)
    {
        if (levelFilter == null) {
            showLine(line);
            return false;
        }

        Matcher m = LEVEL_PATTERN.matcher(line);
        try {
            if (m.find()) {
                String levelName = m.group(1);
                LogLevel level = LogLevel.of(levelName);
                if (levelFilter.toInt() <= level.toInt()) {
                    showLine(line);
                    return false;
                }
                else {
                    return true;
                }
            }
        }
        catch (IllegalArgumentException ex) {
        }

        // unexpected level name or unexpected line format.
        // here assumes that log level with the last line.
        if (!lastLineFiltered) {
            showLine(line);
        }
        return lastLineFiltered;
    }

    private void showLine(String line)
    {
        out.println(line);
    }

    private static LinkedHashMap<String, List<RestLogFileHandle>> sortHandles(List<RestLogFileHandle> handles)
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


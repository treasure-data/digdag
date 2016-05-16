package io.digdag.core.log;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogFileHandle;
import org.msgpack.jackson.dataformat.MessagePackFactory;

import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;

public class LogFiles
{
    static final ObjectMapper MAPPER = new ObjectMapper(new MessagePackFactory());

    private LogFiles()
    { }

    public static final String LOG_GZ_FILE_SUFFIX = ".log.gz";

    private static DateTimeFormatter CREATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd", ENGLISH)
        .withZone(ZoneId.of("UTC"));

    // don't include \ / : * ? " < > | which are not usable on windows
    private static DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssxx", ENGLISH);

    public static String formatDataDir(LogFilePrefix prefix)
    {
        return CREATE_TIME_FORMATTER.format(prefix.getCreatedAt());
    }

    public static String formatSessoinPrefix(LogFilePrefix prefix)
    {
        return String.format(ENGLISH,
                "%d.%d%s@%s",
                prefix.getSiteId(),
                prefix.getProjectId(),
                prefix.getWorkflowName(),
                SESSION_TIME_FORMATTER.withZone(prefix.getTimeZone()).format(prefix.getSessionTime()));
    }

    public static String formatAttemptPrefix(String sessionPrefix, LogFilePrefix prefix)
    {
        return sessionPrefix + prefix.getRetryAttemptName().transform(it -> "_" + it).or("");
    }

    public static String formatSessionAttemptDir(LogFilePrefix prefix)
    {
        return formatAttemptPrefix(formatSessoinPrefix(prefix), prefix);
    }

    public static String formatFileName(String taskName, Instant firstLogTime, String agentId)
    {
        return String.format(ENGLISH,
                "%s@%08x%08x.%s",
                taskName,
                firstLogTime.getEpochSecond(),
                firstLogTime.getNano(),
                agentId) + LOG_GZ_FILE_SUFFIX;
    }

    public static LogFileHandle buildLogFileHandleFromFileName(String fileName, long fileSize)
    {
        // TODO use regexp for reliable parsing logic
        String[] taskNameAndRest = fileName.split("@", 2);
        if (taskNameAndRest.length < 2) {
            return null;
        }
        String taskName = taskNameAndRest[0];

        String[] timeAndRest = taskNameAndRest[1].split("\\.", 2);
        if (timeAndRest.length < 2) {
            return null;
        }

        Instant firstLogTime;
        try {
            long sec = Long.parseLong(timeAndRest[0].substring(0, 8), 16);
            int nsec = Integer.parseInt(timeAndRest[0].substring(8, 16), 16);
            firstLogTime = Instant.ofEpochSecond(sec, nsec);
        }
        catch (NumberFormatException ex) {
            return null;
        }

        String agentId = timeAndRest[1].substring(0, timeAndRest[1].length() - LOG_GZ_FILE_SUFFIX.length());

        return LogFileHandle.builder()
            .fileName(fileName)
            .fileSize(fileSize)
            .taskName(taskName)
            .firstLogTime(firstLogTime)
            .agentId(agentId)
            .direct(Optional.absent())
            .build();
    }

    public static List<LogFileHandle> sortLogFileHandles(List<LogFileHandle> handles)
    {
        Collections.sort(handles, new Comparator<LogFileHandle>() {
            public int compare(LogFileHandle o1, LogFileHandle o2)
            {
                return o1.getFileName().compareTo(o2.getFileName());
            }

            public boolean equals(Object o)
            {
                return o == this;
            }
        });
        return handles;
    }
}

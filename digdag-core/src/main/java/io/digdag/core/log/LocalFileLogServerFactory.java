package io.digdag.core.log;

import java.util.List;
import java.util.Comparator;
import java.util.Collections;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.time.ZoneId;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.digdag.core.agent.AgentId;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogFileHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.client.config.Config;
import java.time.format.DateTimeFormatter;
import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.APPEND;

public class LocalFileLogServerFactory
    implements LogServerFactory
{
    private static final String LOG_GZ_FILE_SUFFIX = ".log.gz";

    private final AgentId agentId;

    @Inject
    public LocalFileLogServerFactory(AgentId agentId)
    {
        this.agentId = agentId;
    }

    @Override
    public String getType()
    {
        return "local";
    }

    @Override
    public LogServer getLogServer(Config systemConfig)
    {
        try {
            Path path = FileSystems.getDefault().getPath(systemConfig.get("log-server.path", String.class, "digdag.log"))
                .toAbsolutePath()
                .normalize();
            return new LocalFileLogServer(path);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private static DateTimeFormatter CREATE_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd", ENGLISH)
        .withZone(ZoneId.of("UTC"));

    // don't include \ / : * ? " < > | which are not usable on windows
    private static DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyyMMdd'T'HHmmssxx", ENGLISH);

    class LocalFileLogServer
        implements LogServer
    {
        private final Path logPath;

        public LocalFileLogServer(Path logPath)
            throws IOException
        {
            Files.createDirectories(logPath);
            this.logPath = logPath;
        }

        @Override
        public String putFile(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId, byte[] gzData)
        {
            try {
                String fileName = buildFileName(taskName, fileTime, nodeId);
                Path prefixDir = getPrefixPath(prefix);
                Files.createDirectories(prefixDir);
                Path filePath = prefixDir.resolve(fileName);
                try (OutputStream out = Files.newOutputStream(filePath)) {
                    out.write(gzData);
                }
                return fileName;
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        public Optional<DirectUploadHandle> getDirectUploadHandle(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId)
        {
            return Optional.absent();
        }

        @Override
        public List<LogFileHandle> getFileHandles(LogFilePrefix prefix, Optional<String> taskName)
        {
            Path dir = getPrefixPath(prefix);
            if (!Files.exists(dir)) {
                return new ArrayList<>();
            }

            List<LogFileHandle> handles = new ArrayList<>();
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
                for (Path path : dirStream) {
                    String name = path.getFileName().toString();
                    if (name.endsWith(LOG_GZ_FILE_SUFFIX) && (!taskName.isPresent() || name.startsWith(taskName.get()))) {
                        LogFileHandle handle = parseFileName(name);
                        if (handle != null) {
                            handles.add(handle);
                        }
                    }
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }

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

        @Override
        public byte[] getFile(LogFilePrefix prefix, String fileName)
            throws FileNotFoundException
        {
            try {
                Path filePath = getPrefixPath(prefix).resolve(fileName);
                try (InputStream in = Files.newInputStream(filePath)) {
                    return ByteStreams.toByteArray(in);
                }
            }
            catch (FileNotFoundException ex) {
                throw ex;
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private Path getPrefixPath(LogFilePrefix prefix)
        {
            String dateDir = CREATE_TIME_FORMATTER.format(prefix.getCreatedAt());
            String sessionPrefix =
                String.format(ENGLISH,
                    "%d.%s%s@%s",
                    prefix.getSiteId(),
                    prefix.getRepositoryName(),
                    prefix.getWorkflowName(),
                    SESSION_TIME_FORMATTER.withZone(prefix.getTimeZone()).format(prefix.getSessionTime()));
            String attemptPrefix = sessionPrefix + prefix.getRetryAttemptName().transform(it -> "_" + it).or("");
            return logPath.resolve(dateDir).resolve(attemptPrefix);
        }

        private String buildFileName(String taskName, Instant fileTime, String nodeId)
        {
            return String.format(ENGLISH,
                    "%s@%08x%08x.%s",
                    taskName,
                    fileTime.getEpochSecond(),
                    fileTime.getNano(),
                    nodeId) + LOG_GZ_FILE_SUFFIX;
        }

        private LogFileHandle parseFileName(String fileName)
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

            Instant fileTime;
            try {
                long sec = Long.parseLong(timeAndRest[0].substring(0, 8), 16);
                int nsec = Integer.parseInt(timeAndRest[0].substring(8, 16), 16);
                fileTime = Instant.ofEpochSecond(sec, nsec);
            }
            catch (NumberFormatException ex) {
                return null;
            }

            String agentId = timeAndRest[1].substring(0, timeAndRest[1].length() - LOG_GZ_FILE_SUFFIX.length());

            return LogFileHandle.builder()
                .fileName(fileName)
                .taskName(taskName)
                .fileTime(fileTime)
                .agentId(agentId)
                .direct(Optional.absent())
                .build();
        }

        public LocalFileDirectTaskLogger newDirectTaskLogger(LogFilePrefix prefix, String taskName)
        {
            try {
                return new LocalFileDirectTaskLogger(prefix, taskName);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        class LocalFileDirectTaskLogger
            implements TaskLogger
        {
            private final OutputStream output;

            public LocalFileDirectTaskLogger(LogFilePrefix prefix, String taskName)
                throws IOException
            {
                Path prefixDir = getPrefixPath(prefix);
                Files.createDirectories(prefixDir);
                Path filePath = prefixDir.resolve(buildFileName(taskName, Instant.now(), agentId.toString()));
                this.output = new GZIPOutputStream(Files.newOutputStream(filePath, CREATE, APPEND), 16*1024);
            }

            @Override
            public void log(LogLevel level, long timestamp, String message)
            {
                byte[] data = message.getBytes(UTF_8);
                log(data, 0, data.length);
            }

            @Override
            public void log(byte[] data, int off, int len)
            {
                try {
                    output.write(data, off, len);
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }

            @Override
            public void close()
            {
                try {
                    output.close();
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }
    }
}

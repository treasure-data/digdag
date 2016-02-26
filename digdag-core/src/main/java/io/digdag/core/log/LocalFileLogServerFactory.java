package io.digdag.core.log;

import java.util.List;
import java.util.zip.GZIPOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.Writer;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.io.FileNotFoundException;
import java.time.Instant;
import java.time.ZoneId;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.file.DirectoryStream;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
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

    private static DateTimeFormatter SESSION_TIME_FORMATTER =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

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
            ImmutableList.Builder<LogFileHandle> builder = ImmutableList.builder();
            Path dir = getPrefixPath(prefix);
            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
                for (Path path : dirStream) {
                    String name = path.getFileName().toString();
                    if (name.endsWith(LOG_GZ_FILE_SUFFIX)) {
                        builder.add(LogFileHandle.ofNonDirect(name));
                    }
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
            return builder.build();
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
            String sessionPrefix =
                String.format(ENGLISH,
                    "%s/%d:%s%s:%s",
                    CREATE_TIME_FORMATTER.format(prefix.getCreatedAt()),
                    prefix.getSiteId(),
                    prefix.getRepositoryName(),
                    prefix.getWorkflowName(),
                    SESSION_TIME_FORMATTER.withZone(prefix.getTimeZone()).format(prefix.getSessionTime()));
            String attemptPrefix = sessionPrefix + prefix.getRetryAttemptName().transform(it -> ":" + it).or("");
            return logPath.resolve(attemptPrefix);
        }

        private String buildFileName(String taskName, Instant fileTime, String nodeId)
        {
            return String.format(ENGLISH,
                    "%s:%08x%08x:%s",
                    taskName,
                    fileTime.getEpochSecond(),
                    fileTime.getNano(),
                    nodeId) + LOG_GZ_FILE_SUFFIX;
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
            private final Writer writer;

            public LocalFileDirectTaskLogger(LogFilePrefix prefix, String taskName)
                throws IOException
            {
                Path prefixDir = getPrefixPath(prefix);
                Files.createDirectories(prefixDir);
                Path filePath = prefixDir.resolve(buildFileName(taskName, Instant.now(), agentId.toString()));
                this.writer = new OutputStreamWriter(new GZIPOutputStream(Files.newOutputStream(filePath, CREATE, APPEND)), UTF_8);
            }

            public void log(LogLevel level, long timestamp, String message)
            {
                try {
                    writer.append(message);
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }

            @Override
            public void close()
            {
                try {
                    writer.close();
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }
    }
}

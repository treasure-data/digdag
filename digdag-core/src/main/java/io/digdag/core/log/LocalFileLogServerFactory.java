package io.digdag.core.log;

import java.util.List;
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
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.StorageFileNotFoundException;
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

    private final Path logPath;
    private final AgentId agentId;

    @Inject
    public LocalFileLogServerFactory(Config systemConfig, AgentId agentId)
    {
        this.logPath = FileSystems.getDefault().getPath(systemConfig.get("log-server.local.path", String.class, "digdag.log"))
            .toAbsolutePath()
            .normalize();
        this.agentId = agentId;
    }

    @Override
    public String getType()
    {
        return "local";
    }

    @Override
    public LogServer getLogServer()
    {
        try {
            return new LocalFileLogServer(logPath);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    class LocalFileLogServer
            extends AbstractFileLogServer
    {
        private final Path logPath;

        public LocalFileLogServer(Path logPath)
            throws IOException
        {
            this.logPath = logPath;
        }

        @Override
        public Optional<DirectUploadHandle> getDirectUploadHandle(String dateDir, String attemptDir, String fileName)
        {
            return Optional.absent();
        }

        @Override
        protected void putFile(String dateDir, String attemptDir, String fileName, byte[] gzData)
        {
            Path dir = getPrefixDir(dateDir, attemptDir);
            try {
                Files.createDirectories(dir);
                Path path = dir.resolve(fileName);
                try (OutputStream out = Files.newOutputStream(path)) {
                    out.write(gzData);
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        protected void listFiles(String dateDir, String attemptDir, boolean enableDirectDownload, FileMetadataConsumer consumer)
        {
            Path dir = getPrefixDir(dateDir, attemptDir);
            if (!Files.exists(dir)) {
                return;
            }

            try (DirectoryStream<Path> ds = Files.newDirectoryStream(dir)) {
                for (Path path : ds) {
                    consumer.accept(
                            path.getFileName().toString(),
                            Files.size(path),
                            null);
                }
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        @Override
        protected byte[] getFile(String dateDir, String attemptDir, String fileName)
            throws StorageFileNotFoundException
        {
            Path path = getPrefixDir(dateDir, attemptDir).resolve(fileName);
            try (InputStream in = Files.newInputStream(path)) {
                return ByteStreams.toByteArray(in);
            }
            catch (FileNotFoundException ex) {
                throw new StorageFileNotFoundException(ex);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        private Path getPrefixDir(String dateDir, String attemptDir)
        {
            return logPath.resolve(dateDir).resolve(attemptDir);
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
                String dateDir = LogFiles.formatDataDir(prefix);
                String attemptDir = LogFiles.formatSessionAttemptDir(prefix);
                String fileName = LogFiles.formatFileName(taskName, Instant.now(), agentId.toString());

                Path dir = getPrefixDir(dateDir, attemptDir);
                Files.createDirectories(dir);
                Path path = dir.resolve(fileName);

                this.output = new GZIPOutputStream(Files.newOutputStream(path, CREATE, APPEND), 16*1024);
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

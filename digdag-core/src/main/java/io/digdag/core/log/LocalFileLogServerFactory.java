package io.digdag.core.log;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.core.agent.AgentId;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;

import static io.digdag.core.log.LogFiles.MAPPER;
import static io.digdag.core.log.TaskLogger.Stream.LOG;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;

public class LocalFileLogServerFactory
        implements LogServerFactory
{
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
        protected void listFiles(String dateDir, String attemptDir, FileMetadataConsumer consumer)
        {
            Path dir = getPrefixDir(dateDir, attemptDir);
            if (!Files.exists(dir)) {
                return;
            }

            try (DirectoryStream<Path> dirStream = Files.newDirectoryStream(dir)) {
                for (Path path : dirStream) {
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
                throws FileNotFoundException
        {
            Path path = getPrefixDir(dateDir, attemptDir).resolve(fileName);
            try (InputStream in = Files.newInputStream(path)) {
                return ByteStreams.toByteArray(in);
            }
            catch (FileNotFoundException ex) {
                throw ex;
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

                this.output = new GZIPOutputStream(Files.newOutputStream(path, CREATE, APPEND), 16 * 1024);
            }

            @Override
            public void write(LogEntry entry)
            {
                try {
                    MAPPER.writeValue(output, entry);
                }
                catch (IOException e) {
                    throw Throwables.propagate(e);
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

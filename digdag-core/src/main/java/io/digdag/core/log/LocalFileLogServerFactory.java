package io.digdag.core.log;

import java.nio.file.FileAlreadyExistsException;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.time.Instant;
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
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.client.config.Config;

import static java.nio.charset.StandardCharsets.UTF_8;

public class LocalFileLogServerFactory
    implements LogServerFactory
{
    private static final String LOG_GZ_FILE_SUFFIX = ".log.gz";

    private final Path logPath;
    private final long logSplitSize;
    private final AgentId agentId;

    @Inject
    public LocalFileLogServerFactory(Config systemConfig, AgentId agentId)
    {
        this.logPath = FileSystems.getDefault().getPath(systemConfig.get("log-server.local.path", String.class, "digdag.log"))
            .toAbsolutePath()
            .normalize();
        this.agentId = agentId;
        this.logSplitSize = systemConfig.get("log-server.local.split_size", Long.class, 0L);
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
        private final ReentrantReadWriteLock lock;
        private final ReentrantReadWriteLock.ReadLock logAppendLock;

        public LocalFileLogServer(Path logPath)
            throws IOException
        {
            this.logPath = logPath;
            this.lock = new ReentrantReadWriteLock();
            this.logAppendLock = lock.readLock();
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
                return new LocalFileDirectTaskLogger(prefix, taskName, logSplitSize);
            }
            catch (IOException ex) {
                throw Throwables.propagate(ex);
            }
        }

        class LocalFileDirectTaskLogger
            implements TaskLogger
        {
            private CountingLogOutputStream output;
            private final long splitSize;;

            private final LogFilePrefix prefix;
            private final String taskName;

            public LocalFileDirectTaskLogger(LogFilePrefix prefix, String taskName, Long splitSize)
                throws IOException
            {
                this.prefix = prefix;
                this.taskName = taskName;
                this.splitSize = splitSize;

                this.output = createOutput(prefix, taskName, agentId);
            }

            protected CountingLogOutputStream createOutput(LogFilePrefix prefix, String taskName, AgentId agentId)
                    throws IOException
            {
                String dateDir = LogFiles.formatDataDir(prefix);
                String attemptDir = LogFiles.formatSessionAttemptDir(prefix);
                String fileName = LogFiles.formatFileName(taskName, Instant.now(), agentId.toString());

                Path dir = getPrefixDir(dateDir, attemptDir);
                if (!Files.exists(dir)) {
                    try {
                        Files.createDirectories(dir);
                    }
                    catch (FileAlreadyExistsException e) {
                        // do nothing
                    }
                }
                Path path = dir.resolve(fileName);
                return new CountingLogOutputStream(path);
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
                write(data, off, len);
            }

            protected void write(byte[] data, int off, int len)
            {
                try {
                    logAppendLock.lock();
                    try {
                        output.write(data, off, len);
                        if (splitSize > 0 && output.getUncompressedSize() > splitSize) {
                            switchLogFile();
                        }
                    }
                    finally {
                        logAppendLock.unlock();
                    }
                }
                catch (IOException ex) {
                    // here can do almost nothing. adding logs to logger causes infinite loop
                    throw Throwables.propagate(ex);
                }
            }

            private void switchLogFile()
                    throws IOException
            {
                output.close();
                output = createOutput(prefix, taskName, agentId);
            }


            @Override
            public void close()
            {
                logAppendLock.lock();
                try {
                    output.close();
                }
                catch (IOException ex) {
                    throw Throwables.propagate(ex);
                }
                finally {
                    logAppendLock.unlock();
                }
            }
        }
    }
}

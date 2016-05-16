package io.digdag.core.log;

import java.time.Instant;
import java.util.Arrays;
import java.util.zip.GZIPOutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.base.Throwables;
import com.google.common.io.ByteStreams;
import io.digdag.core.TempFileManager;

import static io.digdag.core.log.TaskLogger.Stream.LOG;
import static java.nio.charset.StandardCharsets.UTF_8;

public class BufferedRemoteTaskLogger
    implements TaskLogger
{
    public static interface Uploader
    {
        void upload(Instant firstLogTime, byte[] gzData);
    }

    private static final int UPLOAD_THRESHOLD = 16 * 1024 * 1024;

    private final TempFileManager tempFiles;
    private final String tempFilePrefix;
    private final Uploader uploader;
    private final ReentrantReadWriteLock lock;
    private final ReadLock logAppendLock;
    private final WriteLock logUploadLock;
    private volatile CountingLogOutputStream currentFile = null;
    private volatile boolean closed;

    public BufferedRemoteTaskLogger(TempFileManager tempFiles, String tempFilePrefix,
            Uploader uploader)
    {
        this.tempFiles = tempFiles;
        this.tempFilePrefix = tempFilePrefix;
        this.uploader = uploader;
        this.lock = new ReentrantReadWriteLock();
        this.logAppendLock = lock.readLock();
        this.logUploadLock = lock.writeLock();
    }

    @Override
    public void write(LogEntry entry)
    {
        try {
            boolean uploadAfterUnlock = false;
            logAppendLock.lock();
            try {
                openCurrentFile();
                LogFiles.MAPPER.writeValue(currentFile, entry);
                if (currentFile.getUncompressedSize() > UPLOAD_THRESHOLD) {
                    uploadAfterUnlock = true;
                }
            }
            finally {
                logAppendLock.unlock();
            }
            if (uploadAfterUnlock) {
                tryUpload(false);
            }
        }
        catch (IOException ex) {
            // here can do almost nothing. adding logs to logger causes infinite loop
            throw Throwables.propagate(ex);
        }
    }

    private void openCurrentFile()
        throws IOException
    {
        if (currentFile == null) {
            synchronized (this) {
                if (currentFile == null) {
                    if (closed) {
                        throw new IOException("Task logger is already closed");
                    }
                    currentFile = new CountingLogOutputStream(
                            tempFiles.createTempFile("logs", tempFilePrefix, ".log.gz").get()
                            );
                }
            }
        }
    }

    private void tryUpload(boolean atClose)
        throws IOException
    {
        logUploadLock.lock();
        try {
            if (currentFile != null && (atClose || currentFile.getUncompressedSize() > UPLOAD_THRESHOLD)) {
                currentFile.close();
                Path path = currentFile.getPath();
                Instant firstLogTime = currentFile.getOpenTime();
                currentFile = null;

                byte[] gzData;
                try (InputStream in = Files.newInputStream(path)) {
                    gzData = ByteStreams.toByteArray(in);
                }

                uploader.upload(firstLogTime, gzData);
                Files.deleteIfExists(path);
            }
            if (atClose) {
                closed = true;
            }
        }
        finally {
            logUploadLock.unlock();
        }
    }

    @Override
    public void close()
    {
        try {
            tryUpload(true);
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    private static class CountingLogOutputStream
        extends GZIPOutputStream
    {
        private final Path path;
        private final Instant openTime;
        private int count;

        public CountingLogOutputStream(Path path)
            throws IOException
        {
            super(Files.newOutputStream(path), 8*1024);
            this.def.setLevel(9);
            this.path = path;
            this.openTime = Instant.now();
        }

        @Override
        public void write(int b) throws IOException
        {
            super.write(b);
            count++;
        }

        @Override
        public void write(byte b[], int off, int len) throws IOException
        {
            super.write(b, off, len);
            count += len;
        }

        public Path getPath()
        {
            return path;
        }

        public Instant getOpenTime()
        {
            return openTime;
        }

        public int getUncompressedSize()
        {
            return count;
        }
    }
}

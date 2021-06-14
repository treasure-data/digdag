package io.digdag.core.log;

import java.time.Instant;
import java.io.InputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

import com.google.common.io.ByteStreams;
import io.digdag.commons.ThrowablesUtil;
import io.digdag.core.TempFileManager;
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
    public void log(LogLevel level, long timestamp, String message)
    {
        byte[] data = message.getBytes(UTF_8);
        log(data, 0, data.length);
    }

    @Override
    public void log(byte[] data, int off, int len)
    {
        try {
            boolean uploadAfterUnlock = false;
            logAppendLock.lock();
            try {
                openCurrentFile();
                currentFile.write(data, off, len);
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
            throw ThrowablesUtil.propagate(ex);
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
            throw ThrowablesUtil.propagate(ex);
        }
    }

}

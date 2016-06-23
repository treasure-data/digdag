package io.digdag.core.log;

import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.FileNotFoundException;
import com.google.common.io.ByteStreams;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.Storage;
import io.digdag.spi.StorageFile;
import io.digdag.spi.StorageFileNotFoundException;

public class StorageFileLogServer
        extends AbstractFileLogServer
{
    private final Storage storage;
    private final String logPath;

    public StorageFileLogServer(Storage storage, String logPath)
    {
        this.storage = storage;
        this.logPath = logPath;
    }

    private String getPrefixDir(String dateDir, String attemptDir)
    {
        return logPath + dateDir + "/" + attemptDir + "/";
    }

    @Override
    public Optional<DirectUploadHandle> getDirectUploadHandle(String dateDir, String attemptDir, String fileName)
    {
        return storage.getDirectUploadHandle(
                getPrefixDir(dateDir, attemptDir) + fileName);
    }

    @Override
    protected void putFile(String dateDir, String attemptDir, String fileName, byte[] gzData)
    {
        String path = getPrefixDir(dateDir, attemptDir) + fileName;
        try {
            storage.put(path, gzData.length, () -> new ByteArrayInputStream(gzData));
        }
        catch (Throwable ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    protected byte[] getFile(String dateDir, String attemptDir, String fileName)
        throws FileNotFoundException
    {
        String path = getPrefixDir(dateDir, attemptDir) + fileName;
        try {
            StorageFile file = storage.open(path);
            try (InputStream in = file.getContentInputStream()) {
                if (file.getContentLength() > 512*1024*1024) {
                    throw new RuntimeException("Non-direct downloding log files larger than 512MB is not supported");
                }
                byte[] data = new byte[(int) file.getContentLength()];
                ByteStreams.readFully(in, data);
                return data;
            }
        }
        catch (StorageFileNotFoundException ex) {
            throw new FileNotFoundException();
        }
        catch (IOException ex) {
            throw Throwables.propagate(ex);
        }
    }

    @Override
    protected void listFiles(String dateDir, String attemptDir, FileMetadataConsumer consumer)
    {
        String dir = getPrefixDir(dateDir, attemptDir);

        storage.list(dir, (chunk) -> {
            chunk.forEach(meta -> {
                    String key = meta.getKey();
                    String fileName = key.substring(dir.length());

                    consumer.accept(
                            fileName,
                            meta.getContentLength(),
                            storage.getDirectDownloadHandle(key).orNull());
            });
        });
    }
}

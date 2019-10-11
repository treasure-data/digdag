package io.digdag.core.log;

import java.util.List;
import java.util.ArrayList;
import java.util.zip.GZIPOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
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
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogFileHandle;
import io.digdag.spi.ImmutableLogFileHandle;
import io.digdag.spi.DirectDownloadHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.client.config.Config;
import java.time.format.DateTimeFormatter;
import static java.util.Locale.ENGLISH;
import static java.nio.charset.StandardCharsets.UTF_8;

public abstract class AbstractFileLogServer
    implements LogServer
{
    public abstract Optional<DirectUploadHandle> getDirectUploadHandle(String dateDir, String attemptDir, String fileName);

    protected abstract void putFile(String dateDir, String attemptDir, String fileName, byte[] gzData);

    protected abstract byte[] getFile(String dateDir, String attemptDir, String fileName)
            throws StorageFileNotFoundException;

    protected abstract void listFiles(String dateDir, String attemptDir, boolean enableDirectDownload, FileMetadataConsumer fileNameConsumer);

    public interface FileMetadataConsumer
    {
        public void accept(String name, long size, DirectDownloadHandle directOrNull);
    }

    @Override
    public Optional<DirectUploadHandle> getDirectUploadHandle(LogFilePrefix prefix, String taskName, Instant firstLogTime, String agentId)
    {
        String dateDir = LogFiles.formatDataDir(prefix);
        String attemptDir = LogFiles.formatSessionAttemptDir(prefix);
        String fileName = LogFiles.formatFileName(taskName, firstLogTime, agentId);
        return getDirectUploadHandle(dateDir, attemptDir, fileName);
    }

    @Override
    public String putFile(LogFilePrefix prefix, String taskName, Instant firstLogTime, String agentId, byte[] gzData)
    {
        String dateDir = LogFiles.formatDataDir(prefix);
        String attemptDir = LogFiles.formatSessionAttemptDir(prefix);
        String fileName = LogFiles.formatFileName(taskName, firstLogTime, agentId);

        putFile(dateDir, attemptDir, fileName, gzData);

        return fileName;
    }

    @Override
    public byte[] getFile(LogFilePrefix prefix, String fileName)
            throws StorageFileNotFoundException
    {
        String dateDir = LogFiles.formatDataDir(prefix);
        String attemptDir = LogFiles.formatSessionAttemptDir(prefix);
        return getFile(dateDir, attemptDir, fileName);
    }

    @Override
    public List<LogFileHandle> getFileHandles(LogFilePrefix prefix, Optional<String> taskName, boolean enableDirectDownload)
    {
        String dateDir = LogFiles.formatDataDir(prefix);
        String attemptDir = LogFiles.formatSessionAttemptDir(prefix);

        List<LogFileHandle> handles = new ArrayList<>();

        listFiles(dateDir, attemptDir, enableDirectDownload, (name, size, direct) -> {
            if (name.endsWith(LogFiles.LOG_GZ_FILE_SUFFIX) && (!taskName.isPresent() || name.startsWith(taskName.get()))) {
                LogFileHandle handle = LogFiles.buildLogFileHandleFromFileName(name, size);
                if (handle != null) {
                    if (direct != null) {
                        handles.add(
                                ImmutableLogFileHandle.builder()
                                .from(handle)
                                .direct(Optional.of(direct))
                                .build());
                    }
                    else {
                        handles.add(handle);
                    }
                }
            }
        });

        LogFiles.sortLogFileHandles(handles);

        return handles;
    }
}

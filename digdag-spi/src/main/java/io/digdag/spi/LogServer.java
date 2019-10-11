package io.digdag.spi;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;

public interface LogServer
{
    String putFile(LogFilePrefix prefix, String taskName, Instant firstLogTime, String agentId, byte[] gzData);

    Optional<DirectUploadHandle> getDirectUploadHandle(LogFilePrefix prefix, String taskName, Instant firstLogTime, String agentId);

    List<LogFileHandle> getFileHandles(LogFilePrefix prefix, Optional<String> taskName, boolean enableDirectDownload);

    byte[] getFile(LogFilePrefix prefix, String fileName)
        throws StorageFileNotFoundException;
}

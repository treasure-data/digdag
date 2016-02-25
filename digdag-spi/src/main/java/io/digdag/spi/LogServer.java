package io.digdag.spi;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;

public interface LogServer
{
    String putFile(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId, byte[] gzData);

    Optional<DirectUploadHandle> getDirectUploadHandle(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId);

    List<LogFileHandle> getFileHandles(LogFilePrefix prefix, Optional<String> taskName);

    byte[] getFile(LogFilePrefix prefix, String fileName);
}

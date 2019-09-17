package io.digdag.core.log;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import com.google.common.base.Optional;
import io.digdag.spi.LogServer;
import io.digdag.spi.LogServerFactory;
import io.digdag.spi.LogFilePrefix;
import io.digdag.spi.LogFileHandle;
import io.digdag.spi.DirectUploadHandle;
import io.digdag.spi.StorageFileNotFoundException;
import io.digdag.client.config.Config;

public class NullLogServerFactory
    implements LogServerFactory
{
    @Inject
    public NullLogServerFactory()
    { }

    @Override
    public String getType()
    {
        return "null";
    }

    @Override
    public LogServer getLogServer()
    {
        return new NullLogServer();
    }

    static class NullLogServer
        implements LogServer
    {
        @Override
        public String putFile(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId, byte[] gzData)
        {
            return "null";
        }

        @Override
        public Optional<DirectUploadHandle> getDirectUploadHandle(LogFilePrefix prefix, String taskName, Instant fileTime, String nodeId)
        {
            return Optional.absent();
        }

        @Override
        public List<LogFileHandle> getFileHandles(LogFilePrefix prefix, Optional<String> taskName, boolean enableDirectDownload)
        {
            return ImmutableList.of();
        }

        @Override
        public byte[] getFile(LogFilePrefix prefix, String fileName)
            throws StorageFileNotFoundException
        {
            throw new StorageFileNotFoundException("Log file not found");
        }
    }
}

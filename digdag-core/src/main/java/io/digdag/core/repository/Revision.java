package io.digdag.core.repository;

import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;

@JsonDeserialize(as = ImmutableRevision.class)
public abstract class Revision
{
    public abstract String getName();

    public abstract Config getGlobalParams();

    public abstract String getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();

    public abstract Optional<String> getArchivePath();

    public abstract Optional<byte[]> getArchiveData();

    public static ImmutableRevision.Builder revisionBuilder()
    {
        return ImmutableRevision.builder();
    }

    @Value.Check
    protected void check()
    {
        // TODO check name
    }
}

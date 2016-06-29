package io.digdag.core.repository;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.archive.ArchiveMetadata;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableRevision.class)
public abstract class Revision
{
    public abstract String getName();

    public abstract Config getDefaultParams();

    public abstract ArchiveType getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();

    public abstract Optional<String> getArchivePath();

    public abstract Config getUserInfo();

    public static Revision copyOf(Revision other)
    {
        return ImmutableRevision.builder().from(other).build();
    }

    public static ImmutableRevision.Builder builderFromArchive(String name, ArchiveMetadata meta, Config userInfo)
    {
        return ImmutableRevision.builder()
            .name(name)
            .defaultParams(meta.getDefaultParams().deepCopy())
            .userInfo(userInfo);
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkIdentifierName("name", getName())
            .validate("revision", this);
    }
}

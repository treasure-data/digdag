package io.digdag.core.repository;

import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@JsonDeserialize(as = ImmutableRevision.class)
public abstract class Revision
{
    public abstract String getName();

    public abstract Config getDefaultParams();

    public abstract String getArchiveType();

    public abstract Optional<byte[]> getArchiveMd5();

    public abstract Optional<String> getArchivePath();

    public static Revision copyOf(Revision other)
    {
        return ImmutableRevision.builder().from(other).build();
    }

    public static ImmutableRevision.Builder builderFromArchive(String name, ArchiveMetadata meta)
    {
        return ImmutableRevision.builder()
            .name(name)
            .defaultParams(
                    meta.getDefaultParams().deepCopy().set("timezone", meta.getDefaultTimeZone()));
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkIdentifierName("name", getName())
            .validate("revision", this);
    }
}

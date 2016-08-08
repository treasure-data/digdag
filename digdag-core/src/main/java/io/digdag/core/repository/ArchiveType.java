package io.digdag.core.repository;

import java.util.Objects;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class ArchiveType
{
    public static final ArchiveType DB = ArchiveType.of("db");

    public static final ArchiveType NONE = ArchiveType.of("none");

    @JsonCreator
    public static ArchiveType of(String name)
    {
        return new ArchiveType(name);
    }

    private String name;

    private ArchiveType(String name)
    {
        this.name = Objects.requireNonNull(name);
    }

    @JsonValue
    public String getName()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (!(obj instanceof ArchiveType)) {
            return false;
        }
        ArchiveType o = (ArchiveType) obj;
        return Objects.equals(this.name, o.name);
    }

    @Override
    public String toString()
    {
        return Objects.toString(name);
    }
}

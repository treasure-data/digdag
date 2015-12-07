package io.digdag.spi;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

public class RevisionInfo
{
    @JsonCreator
    public static RevisionInfo of(
            @JsonProperty("id") int id,
            @JsonProperty("repositoryName") String repositoryName,
            @JsonProperty("name") String name)
    {
        return new RevisionInfo(id, repositoryName, name);
    }

    private final int id;
    private final String repositoryName;
    private final String name;

    private RevisionInfo(int id, String repositoryName, String name)
    {
        this.id = id;
        this.repositoryName = repositoryName;
        this.name = name;
    }

    @JsonProperty("id")
    public int getInternalId()
    {
        return id;
    }

    @JsonProperty("repositoryName")
    public String getSessionName()
    {
        return repositoryName;
    }

    @JsonProperty("name")
    public String getName()
    {
        return name;
    }

    @Override
    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof RevisionInfo && equalTo((RevisionInfo) another));
    }

    private boolean equalTo(RevisionInfo another)
    {
        return id == id
            && repositoryName.equals(another.repositoryName)
            && name.equals(another.name);
    }

    @Override
    public int hashCode() {
        int h = 31;
        h = h * 17 + id;
        h = h * 17 + repositoryName.hashCode();
        h = h * 17 + name.hashCode();
        return h;
    }
}

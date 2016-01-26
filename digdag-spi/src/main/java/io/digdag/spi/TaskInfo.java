package io.digdag.spi;

import com.google.common.base.Optional;
import com.google.common.primitives.Longs;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonCreator;

public class TaskInfo
{
    @JsonCreator
    public static TaskInfo of(
            @JsonProperty("id") long id,
            @JsonProperty("siteId") int siteId,
            @JsonProperty("sessionId") long sessionId,
            @JsonProperty("retryAttemptName") Optional<String> retryAttemptName,
            @JsonProperty("fullName") String fullName)
    {
        return new TaskInfo(id, siteId, sessionId, retryAttemptName, fullName);
    }

    private final long id;
    private final int siteId;
    private final long sessionId;
    private final Optional<String> retryAttemptName;
    private final String fullName;

    private TaskInfo(long id, int siteId, long sessionId,
            Optional<String> retryAttemptName, String fullName)
    {
        this.id = id;
        this.siteId = siteId;
        this.sessionId = sessionId;
        this.retryAttemptName = retryAttemptName;
        this.fullName = fullName;
    }

    @JsonProperty("id")
    public long getId()
    {
        return id;
    }

    @JsonProperty("siteId")
    public int getSiteId()
    {
        return siteId;
    }

    @JsonProperty("sessionId")
    public long getSessionId()
    {
        return sessionId;
    }

    @JsonProperty("retryAttemptName")
    public Optional<String> getSessionName()
    {
        return retryAttemptName;
    }

    @JsonProperty("fullName")
    public String getFullName()
    {
        return fullName;
    }

    @Override
    public boolean equals(Object another)
    {
        return this == another ||
            (another instanceof TaskInfo && equalTo((TaskInfo) another));
    }

    private boolean equalTo(TaskInfo another)
    {
        return id == id
            && siteId == siteId
            && sessionId == sessionId
            && retryAttemptName.equals(another.retryAttemptName)
            && fullName.equals(another.fullName);
    }

    @Override
    public int hashCode() {
        int h = 31;
        h = h * 17 + Longs.hashCode(id);
        h = h * 17 + siteId;
        h = h * 17 + Longs.hashCode(sessionId);
        h = h * 17 + retryAttemptName.hashCode();
        h = h * 17 + fullName.hashCode();
        return h;
    }
}

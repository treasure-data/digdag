package io.digdag.spi;

import org.immutables.value.Value;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.spi.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskRequest.class)
@JsonDeserialize(as = ImmutableTaskRequest.class)
public abstract class TaskRequest
{
    public abstract TaskInfo getTaskInfo();

    public abstract Optional<RevisionInfo> getRevisionInfo();

    public abstract Config getConfig();

    public abstract Config getLastStateParams();

    public static ImmutableTaskRequest.Builder builder()
    {
        return ImmutableTaskRequest.builder();
    }
}

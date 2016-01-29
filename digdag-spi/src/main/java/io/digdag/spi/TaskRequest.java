package io.digdag.spi;

import org.immutables.value.Value;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskRequest.class)
@JsonDeserialize(as = ImmutableTaskRequest.class)
public abstract class TaskRequest
{
    public abstract TaskInfo getTaskInfo();

    public abstract int getRepositoryId();

    public abstract String getWorkflowName();

    public abstract Optional<String> getRevision();

    public abstract Optional<String> getDagfilePath();

    public abstract Config getLocalConfig();

    public abstract Config getConfig();

    public abstract Config getLastStateParams();

    public static ImmutableTaskRequest.Builder builder()
    {
        return ImmutableTaskRequest.builder();
    }
}

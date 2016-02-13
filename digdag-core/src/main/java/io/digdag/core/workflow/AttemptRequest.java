package io.digdag.core.workflow;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.session.SessionMonitor;

@Value.Immutable
@JsonSerialize(as = ImmutableAttemptRequest.class)
@JsonDeserialize(as = ImmutableAttemptRequest.class)
public abstract class AttemptRequest
{
    @Value.Immutable
    @JsonSerialize(as = ImmutableStored.class)
    @JsonDeserialize(as = ImmutableStored.class)
    public static abstract class Stored
    {
        public abstract long getWorkflowDefinitionId();

        public abstract int getRevisionId();

        public abstract String getRevisionName();

        public abstract int getRepositoryId();

        public abstract Config getRevisionDefaultParams();

        public static Stored of(StoredRevision rev, StoredWorkflowDefinition def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .revisionId(rev.getId())
                .revisionName(rev.getName())
                .repositoryId(rev.getRepositoryId())
                .revisionDefaultParams(rev.getDefaultParams())
                .build();
        }

        public static Stored of(StoredWorkflowDefinitionWithRepository def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .revisionId(def.getRevisionId())
                .revisionName(def.getRevisionName())
                .repositoryId(def.getRepository().getId())
                .revisionDefaultParams(def.getRevisionDefaultParams())
                .build();
        }
    }

    public abstract Stored getStored();

    public abstract String getWorkflowName();

    public abstract Instant getInstant();

    public abstract ZoneId getTimeZone();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Config getSessionParams();

    public abstract List<SessionMonitor> getSessionMonitors();

    @Value.Check
    protected void check()
    {
        // TODO if getRetryAttemptName().isPresent, get() should not be empty
    }
}

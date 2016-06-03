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
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.repository.ModelValidator;

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

        public abstract int getProjectId();

        public static Stored of(StoredRevision rev, StoredWorkflowDefinition def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .projectId(rev.getProjectId())
                .build();
        }

        public static Stored of(StoredWorkflowDefinitionWithProject def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .projectId(def.getProject().getId())
                .build();
        }
    }

    // TODO to support one-time non-stored workflows, this should be Optional<Stored>. See also Session.getProjectId.
    public abstract Stored getStored();

    public abstract String getWorkflowName();

    public abstract Instant getSessionTime();

    public abstract ZoneId getTimeZone();

    public abstract Config getSessionParams();

    public abstract List<SessionMonitor> getSessionMonitors();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Optional<Long> getResumingAttemptId();

    public abstract List<Long> getResumingTasks();

    @Value.Check
    protected void check()
    {
        ModelValidator validator = ModelValidator.builder();
        if (getRetryAttemptName().isPresent()) {
            validator.checkIdentifierName("retry attempt name", getRetryAttemptName().get());
        }
        if (!getResumingAttemptId().isPresent()) {
            validator.check("resuming attempt id list", getResumingTasks(), getResumingTasks().isEmpty(), "must be empty if resuming attempt id is not set");
        }
        validator.validate("attempt request", this);
    }
}

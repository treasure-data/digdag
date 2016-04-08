package io.digdag.core.workflow;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.core.repository.PackageName;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
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

        public abstract int getRepositoryId();

        public static Stored of(StoredRevision rev, StoredWorkflowDefinition def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .repositoryId(rev.getRepositoryId())
                .build();
        }

        public static Stored of(StoredWorkflowDefinitionWithRepository def)
        {
            return ImmutableStored.builder()
                .workflowDefinitionId(def.getId())
                .repositoryId(def.getRepository().getId())
                .build();
        }
    }

    // TODO to support one-time non-stored workflows, this should be Optional<Stored>. See also Session.getRepositoryId.
    public abstract Stored getStored();

    public abstract PackageName getPackageName();

    public abstract String getWorkflowName();

    public abstract Instant getSessionTime();

    public abstract ZoneId getTimeZone();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Config getSessionParams();

    public abstract List<SessionMonitor> getSessionMonitors();

    @Value.Check
    protected void check()
    {
        ModelValidator validator = ModelValidator.builder();
        if (getRetryAttemptName().isPresent()) {
            validator.checkIdentifierName("retry attempt name", getRetryAttemptName().get());
        }
        validator.validate("attempt request", this);
    }
}

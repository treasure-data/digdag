package io.digdag.server;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.spi.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSessionRequest.class)
@JsonDeserialize(as = ImmutableRestSessionRequest.class)
public abstract class RestSessionRequest
{
    @JsonProperty("workflow")
    public abstract String getWorkflowName();

    // TODO Optional doesn't work with JAX-RS somehow
    //@JsonProperty("repository")
    //public abstract Optional<String> getRepositoryName();

    //@JsonProperty("workflow")
    //public abstract Optional<String> getWorkflowName();

    //@JsonProperty("revision")
    //public abstract Optional<String> getRevision();

    //@JsonProperty("workflowId")
    //public abstract Optional<Integer> getWorkflowId();

    //@JsonProperty("session_name")
    //public abstract Optional<String> getSessionName();

    //@JsonProperty("params")
    //public abstract Optional<Config> getSessionParams();

    //@JsonProperty("from")
    //public abstract Optional<String> getFromTaskName();
}

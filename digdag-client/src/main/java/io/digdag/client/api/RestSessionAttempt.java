package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.immutables.value.Value;

@Value.Immutable
@JsonDeserialize(as = ImmutableRestSessionAttempt.class)
public interface RestSessionAttempt {
    Id getId();

    @Value.Default
    default int getIndex() {
        // index has default value only to keep backward compatibility.
        // index is added since 01714615088e315ba401f4deca41b6dd58bb349b.
        return 0;
    }

    IdAndName getProject();

    // Optional<String> getRevision();

    NameOptionalId getWorkflow();

    Id getSessionId();

    UUID getSessionUuid();

    OffsetDateTime getSessionTime();

    Optional<String> getRetryAttemptName();

    boolean getDone();

    boolean getSuccess();

    boolean getCancelRequested();

    Config getParams();

    Instant getCreatedAt();

    Optional<Instant> getFinishedAt();

    default String getStatus() {
        if (this.getSuccess()) {
            return "success";
        }
        if (this.getDone()) {
            if (this.getCancelRequested()) {
                return "killed";
            }
            else {
                return "error";
            }
        }
        return "running";
    }

    static ImmutableRestSessionAttempt.Builder builder() {
        return ImmutableRestSessionAttempt.builder();
    }
}

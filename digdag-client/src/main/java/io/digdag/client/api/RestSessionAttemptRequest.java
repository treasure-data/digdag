package io.digdag.client.api;

import java.util.List;
import java.time.Instant;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Optional;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@Value.Enclosing
@JsonSerialize(as = ImmutableRestSessionAttemptRequest.class)
@JsonDeserialize(as = ImmutableRestSessionAttemptRequest.class)
public interface RestSessionAttemptRequest
{
    long getWorkflowId();

    Instant getSessionTime();

    Optional<String> getRetryAttemptName();

    Optional<Resume> getResume();

    Config getParams();

    static ImmutableRestSessionAttemptRequest.Builder builder()
    {
        return ImmutableRestSessionAttemptRequest.builder();
    }

    static RestSessionAttemptRequest copyWithResume(RestSessionAttemptRequest src, Resume resume)
    {
        return RestSessionAttemptRequest.builder().from(src)
            .resume(Optional.of(resume))
            .build();
    }

    static enum Mode {
        FROM("from"),
        FAILED("failed"),
        ;

        private final String name;

        private Mode(String name)
        {
            this.name = name;
        }

        @JsonValue
        public String toString()
        {
            return name;
        }
    }

    @JsonTypeInfo(
            use = JsonTypeInfo.Id.NAME,
            include = JsonTypeInfo.As.PROPERTY,
            property = "mode",
            visible = false)
    @JsonSubTypes({
            @JsonSubTypes.Type(value = ImmutableRestSessionAttemptRequest.ResumeFrom.class, name = "from"),
            @JsonSubTypes.Type(value = ImmutableRestSessionAttemptRequest.ResumeFailed.class, name = "failed"),
    })
    interface Resume
    {
        @JsonProperty("attemptId")
        public long getAttemptId();

        @JsonProperty("mode")
        public Mode getMode();
    }

    @Value.Immutable
    @JsonSerialize(as = RestSessionAttemptRequest.ResumeFrom.class)
    @JsonDeserialize(as = ImmutableRestSessionAttemptRequest.ResumeFrom.class)
    interface ResumeFrom
            extends Resume
    {
        @Override
        default Mode getMode()
        {
            return Mode.FROM;
        }

        @JsonProperty("from")
        String getFromTaskNamePattern();

        static ResumeFrom of(long attemptId, String fromTaskNamePattern)
        {
            return builder()
                .attemptId(attemptId)
                .fromTaskNamePattern(fromTaskNamePattern)
                .build();
        }

        static ImmutableRestSessionAttemptRequest.ResumeFrom.Builder builder()
        {
            return ImmutableRestSessionAttemptRequest.ResumeFrom.builder();
        }
    }

    @Value.Immutable
    @JsonSerialize(as = RestSessionAttemptRequest.ResumeFailed.class)
    @JsonDeserialize(as = ImmutableRestSessionAttemptRequest.ResumeFailed.class)
    interface ResumeFailed
            extends Resume
    {
        @Override
        default Mode getMode()
        {
            return Mode.FAILED;
        }

        static ResumeFailed of(long attemptId)
        {
            return builder()
                .attemptId(attemptId)
                .build();
        }

        static ImmutableRestSessionAttemptRequest.ResumeFailed.Builder builder()
        {
            return ImmutableRestSessionAttemptRequest.ResumeFailed.builder();
        }
    }
}

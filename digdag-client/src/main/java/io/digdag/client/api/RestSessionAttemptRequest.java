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
public abstract class RestSessionAttemptRequest
{
    public abstract long getWorkflowId();

    public abstract Instant getSessionTime();

    public abstract Optional<String> getRetryAttemptName();

    public abstract Optional<Resume> getResume();

    public abstract Config getParams();

    public static ImmutableRestSessionAttemptRequest.Builder builder()
    {
        return ImmutableRestSessionAttemptRequest.builder();
    }

    public RestSessionAttemptRequest withResume(Resume resume)
    {
        return RestSessionAttemptRequest.builder().from(this)
            .resume(Optional.of(resume))
            .build();
    }

    public static enum Mode {
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
    public static abstract class Resume
    {
        @JsonProperty("attemptId")
        public abstract long getAttemptId();

        @JsonProperty("mode")
        public abstract Mode getMode();
    }

    @Value.Immutable
    @JsonSerialize(as = RestSessionAttemptRequest.ResumeFrom.class)
    @JsonDeserialize(as = ImmutableRestSessionAttemptRequest.ResumeFrom.class)
    public static abstract class ResumeFrom
            extends Resume
    {
        @Override
        public abstract long getAttemptId();

        @Override
        public Mode getMode()
        {
            return Mode.FROM;
        }

        @JsonProperty("from")
        public abstract String getFromTaskNamePattern();

        public static ResumeFrom of(long attemptId, String fromTaskNamePattern)
        {
            return builder()
                .attemptId(attemptId)
                .fromTaskNamePattern(fromTaskNamePattern)
                .build();
        }

        public static ImmutableRestSessionAttemptRequest.ResumeFrom.Builder builder()
        {
            return ImmutableRestSessionAttemptRequest.ResumeFrom.builder();
        }
    }

    @Value.Immutable
    @JsonSerialize(as = RestSessionAttemptRequest.ResumeFailed.class)
    @JsonDeserialize(as = ImmutableRestSessionAttemptRequest.ResumeFailed.class)
    public static abstract class ResumeFailed
            extends Resume
    {
        @Override
        public Mode getMode()
        {
            return Mode.FAILED;
        }

        @Override
        public abstract long getAttemptId();

        public static ResumeFailed of(long attemptId)
        {
            return builder()
                .attemptId(attemptId)
                .build();
        }

        public static ImmutableRestSessionAttemptRequest.ResumeFailed.Builder builder()
        {
            return ImmutableRestSessionAttemptRequest.ResumeFailed.builder();
        }
    }
}

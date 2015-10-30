package io.digdag.cli;

import java.util.List;
import java.util.Map;
import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import static com.google.common.base.Preconditions.checkState;
import io.digdag.core.*;

@Value.Immutable
@JsonSerialize(as = ImmutableResumeState.class)
@JsonDeserialize(as = ImmutableResumeState.class)
public abstract class ResumeState
{
    public abstract Map<String, TaskReport> getReports();

    public static ImmutableResumeState.Builder builder()
    {
        return ImmutableResumeState.builder();
    }

    public static ResumeState of(Map<String, TaskReport> reports)
    {
        return builder()
            .reports(reports)
            .build();
    }
}

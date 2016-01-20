package io.digdag.cli;

import io.digdag.spi.TaskReport;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.core.session.TaskStateCode;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskResumeState.class)
@JsonDeserialize(as = ImmutableTaskResumeState.class)
public abstract class TaskResumeState
{
    public abstract String getFullName();

    public abstract TaskStateCode getState();

    public abstract TaskReport getReport();

    public static ImmutableTaskResumeState.Builder builder()
    {
        return ImmutableTaskResumeState.builder();
    }

    public static TaskResumeState of(String fullName, TaskStateCode state, TaskReport report)
    {
        return builder()
            .fullName(fullName)
            .state(state)
            .report(report)
            .build();
    }
}

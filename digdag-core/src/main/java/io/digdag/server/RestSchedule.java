package io.digdag.server;

import java.util.Date;
import com.google.common.base.Optional;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.core.config.Config;
import io.digdag.core.repository.StoredWorkflowSource;
import io.digdag.core.schedule.StoredSchedule;

@Value.Immutable
@JsonSerialize(as = ImmutableRestSchedule.class)
@JsonDeserialize(as = ImmutableRestSchedule.class)
public abstract class RestSchedule
{
    public abstract long getId();

    public abstract IdName getWorkflow();

    public abstract Config getConfig();

    public abstract long getNextRunTime();

    public abstract long getNextScheduleTime();

    public abstract Date getCreatedAt();

    public abstract Date getUpdatedAt();

    public static ImmutableRestSchedule.Builder builder()
    {
        return ImmutableRestSchedule.builder();
    }

    public static RestSchedule of(StoredSchedule sched, StoredWorkflowSource wf)
    {
        return builder()
            .id(sched.getId())
            .config(sched.getConfig())
            .nextRunTime(sched.getNextRunTime().getTime() / 1000)
            .nextScheduleTime(sched.getNextScheduleTime().getTime() / 1000)
            .createdAt(sched.getCreatedAt())
            .updatedAt(sched.getCreatedAt())
            .workflow(IdName.of(wf.getId(), wf.getName()))
            .build();
    }
}

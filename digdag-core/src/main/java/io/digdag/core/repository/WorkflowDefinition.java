package io.digdag.core.repository;

import java.time.ZoneId;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.repository.PackageName;
import io.digdag.core.schedule.SchedulerManager;

@JsonDeserialize(as = ImmutableWorkflowDefinition.class)
public abstract class WorkflowDefinition
{
    public abstract PackageName getPackageName();

    public abstract String getName();

    public abstract Config getConfig();

    public static WorkflowDefinition of(PackageName packageName, String name, Config config)
    {
        return ImmutableWorkflowDefinition.builder()
            .packageName(packageName)
            .name(name)
            .config(config)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkTaskName("name", getName())
            .validate("workflow", this);
    }

    public static ZoneId getTimeZoneOfWorkflow(Revision rev, WorkflowDefinition def)
    {
        return SchedulerManager.getTimeZoneOfWorkflow(rev, def);
    }
}

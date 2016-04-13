package io.digdag.core.repository;

import java.time.ZoneId;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;
import io.digdag.core.schedule.SchedulerManager;

@JsonDeserialize(as = ImmutableWorkflowDefinition.class)
public abstract class WorkflowDefinition
{
    public abstract String getName();

    public abstract Config getConfig();

    public abstract ZoneId getTimeZone();

    public static WorkflowDefinition of(String name, Config config, ZoneId timeZone)
    {
        return ImmutableWorkflowDefinition.builder()
            .name(name)
            .config(config)
            .timeZone(timeZone)
            .build();
    }

    @Value.Check
    protected void check()
    {
        ModelValidator.builder()
            .checkWorkflowName("name", getName())
            .validate("workflow", this);
    }
}

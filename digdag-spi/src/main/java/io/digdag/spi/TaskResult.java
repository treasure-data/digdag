package io.digdag.spi;

import com.google.common.base.*;
import com.google.common.collect.*;
import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import static com.google.common.base.Preconditions.checkState;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskResult.class)
@JsonDeserialize(as = ImmutableTaskResult.class)
public abstract class TaskResult
{
    public abstract Config getSubtaskConfig();

    public abstract Config getExportParams();

    public abstract Config getStoreParams();

    public abstract TaskReport getReport();

    public static ImmutableTaskResult.Builder builder()
    {
        return ImmutableTaskResult.builder()
            .report(TaskReport.empty());
    }

    public static TaskResult empty(ConfigFactory cf)
    {
        return builder()
            .subtaskConfig(cf.create())
            .exportParams(cf.create())
            .storeParams(cf.create())
            .report(TaskReport.empty())
            .build();
    }
}

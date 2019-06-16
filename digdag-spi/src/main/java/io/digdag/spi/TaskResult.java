package io.digdag.spi;

import org.immutables.value.Value;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;
import io.digdag.client.config.ConfigFactory;
import java.util.List;

@Value.Immutable
@JsonSerialize(as = ImmutableTaskResult.class)
@JsonDeserialize(as = ImmutableTaskResult.class)
public interface TaskResult
{
    Config getSubtaskConfig();

    @Value.Default
    default boolean getCallSubTask()
    {
        return false;
    }

    Config getExportParams();

    List<ConfigKey> getResetStoreParams();

    Config getStoreParams();

    TaskReport getReport();

    static ImmutableTaskResult.Builder builder()
    {
        return ImmutableTaskResult.builder();
    }

    static ImmutableTaskResult.Builder defaultBuilder(TaskRequest request)
    {
        return defaultBuilder(request.getConfig().getFactory());
    }

    static ImmutableTaskResult.Builder defaultBuilder(ConfigFactory cf)
    {
        return builder()
            .subtaskConfig(cf.create())
            .exportParams(cf.create())
            .resetStoreParams(ImmutableList.of())
            .storeParams(cf.create())
            .report(TaskReport.empty());
    }

    static TaskResult empty(TaskRequest request)
    {
        return empty(request.getConfig().getFactory());
    }

    static TaskResult empty(ConfigFactory cf)
    {
        return defaultBuilder(cf).build();
    }
}

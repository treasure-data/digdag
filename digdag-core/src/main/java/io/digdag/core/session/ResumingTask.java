package io.digdag.core.session;

import java.time.Instant;
import java.util.List;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.spi.TaskReport;
import io.digdag.core.workflow.TaskConfig;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigKey;

@Value.Immutable
@JsonSerialize(as = ImmutableResumingTask.class)
@JsonDeserialize(as = ImmutableResumingTask.class)
public abstract class ResumingTask
{
    public abstract long getSourceTaskId();

    public abstract String getFullName();

    public abstract TaskConfig getConfig();

    public abstract Instant getUpdatedAt();

    public abstract Config getSubtaskConfig();

    public abstract Config getExportParams();

    public abstract List<ConfigKey> getResetStoreParams();

    public abstract Config getStoreParams();

    public abstract TaskReport getReport();

    public abstract Config getError();

    public static ResumingTask of(ArchivedTask source)
    {
        return ImmutableResumingTask.builder()
            .sourceTaskId(source.getId())
            .fullName(source.getFullName())
            .config(source.getConfig())
            .updatedAt(source.getUpdatedAt())
            .subtaskConfig(source.getSubtaskConfig())
            .exportParams(source.getExportParams())
            .resetStoreParams(source.getResetStoreParams())
            .storeParams(source.getStoreParams())
            .report(source.getReport().or(TaskReport.empty()))
            .error(source.getError())
            .build();
    }
}

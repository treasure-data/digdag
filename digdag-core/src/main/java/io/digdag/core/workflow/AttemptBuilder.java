package io.digdag.core.workflow;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.schedule.SlaCalculator;
import io.digdag.core.session.SessionMonitor;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;

public class AttemptBuilder
{
    private final SchedulerManager schedulerManager;
    private final SlaCalculator slaCalculator;

    @Inject
    public AttemptBuilder(SchedulerManager schedulerManager, SlaCalculator slaCalculator)
    {
        this.schedulerManager = schedulerManager;
        this.slaCalculator = slaCalculator;
    }

    public AttemptRequest buildFromStoredWorkflow(
            Optional<String> retryAttemptName,
            StoredRevision rev,
            StoredWorkflowDefinition def,
            Config overwriteParams,
            ScheduleTime time)
    {
        ZoneId timeZone = ScheduleExecutor.getWorkflowTimeZone(rev.getDefaultParams(), def);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(rev, def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, time.getRunTime(), timeZone))
            .timeZone(timeZone)
            .sessionParams(buildSessionParameters(overwriteParams, def, timeZone))
            .retryAttemptName(retryAttemptName)
            .instant(time.getTime())
            .build();
    }

    public AttemptRequest buildFromStoredWorkflow(
            Optional<String> retryAttemptName,
            StoredWorkflowDefinitionWithRepository def,
            Config overwriteParams,
            ScheduleTime time)
    {
        ZoneId timeZone = ScheduleExecutor.getWorkflowTimeZone(def.getRevisionDefaultParams(), def);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, time.getRunTime(), timeZone))
            .timeZone(timeZone)
            .sessionParams(buildSessionParameters(overwriteParams, def, timeZone))
            .retryAttemptName(retryAttemptName)
            .instant(time.getTime())
            .build();
    }

    private List<SessionMonitor> buildSessionMonitors(WorkflowDefinition def, Instant runTime, ZoneId timeZone)
    {
        // TODO move this to WorkflowExecutor?
        ImmutableList.Builder<SessionMonitor> monitors = ImmutableList.builder();
        if (def.getConfig().has("sla")) {
            Config slaConfig = def.getConfig().getNestedOrGetEmpty("sla");
            // TODO support multiple SLAs
            Instant triggerTime = slaCalculator.getTriggerTime(slaConfig, runTime, timeZone);
            monitors.add(SessionMonitor.of("sla", slaConfig, triggerTime));
        }
        return monitors.build();
    }

    private Config buildSessionParameters(Config overwriteParams, WorkflowDefinition def, ZoneId timeZone)
    {
        Optional<Scheduler> sr = schedulerManager.tryGetScheduler(def, timeZone);
        // TODO calculate next_session_time and current_session_time
        return overwriteParams;
    }
}

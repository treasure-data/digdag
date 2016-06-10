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
import io.digdag.core.repository.StoredWorkflowDefinitionWithProject;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.session.SessionMonitor;
import io.digdag.spi.Scheduler;
import io.digdag.spi.ScheduleTime;
import static io.digdag.core.agent.RuntimeParams.formatSessionTime;

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
            StoredRevision rev,
            StoredWorkflowDefinition def,
            Config overwriteParams,
            ScheduleTime time)
    {
        return buildFromStoredWorkflow(
                rev, def, overwriteParams, time,
                Optional.absent(), Optional.absent(), ImmutableList.of());
    }

    public AttemptRequest buildFromStoredWorkflow(
            StoredRevision rev,
            StoredWorkflowDefinition def,
            Config overwriteParams,
            ScheduleTime time,
            Optional<String> retryAttemptName,
            Optional<Long> resumingAttemptId,
            List<Long> resumingTasks)
    {
        ZoneId timeZone = def.getTimeZone();
        Config sessionParams = buildSessionParameters(overwriteParams, schedulerManager.tryGetScheduler(rev, def), time.getTime(), timeZone);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(rev, def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, time.getRunTime(), timeZone))
            .timeZone(timeZone)
            .sessionParams(sessionParams)
            .retryAttemptName(retryAttemptName)
            .sessionTime(time.getTime())
            .resumingAttemptId(resumingAttemptId)
            .resumingTasks(resumingTasks)
            .build();
    }

    public AttemptRequest buildFromStoredWorkflow(
            StoredWorkflowDefinitionWithProject def,
            Config overwriteParams,
            ScheduleTime time)
    {
        return buildFromStoredWorkflow(
                def, overwriteParams, time,
                Optional.absent(), Optional.absent(), ImmutableList.of());
    }

    public AttemptRequest buildFromStoredWorkflow(
            StoredWorkflowDefinitionWithProject def,
            Config overwriteParams,
            ScheduleTime time,
            Optional<String> retryAttemptName,
            Optional<Long> resumingAttemptId,
            List<Long> resumingTasks)
    {
        ZoneId timeZone = def.getTimeZone();
        Config sessionParams = buildSessionParameters(overwriteParams, schedulerManager.tryGetScheduler(def), time.getTime(), timeZone);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, time.getRunTime(), timeZone))
            .timeZone(timeZone)
            .sessionParams(sessionParams)
            .retryAttemptName(retryAttemptName)
            .sessionTime(time.getTime())
            .resumingAttemptId(resumingAttemptId)
            .resumingTasks(resumingTasks)
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

    private Config buildSessionParameters(Config overwriteParams, Optional<Scheduler> sr, Instant sessionTime, ZoneId timeZone)
    {
        Config params = overwriteParams.deepCopy();
        if (sr.isPresent()) {
            Instant lastSessionTime = sr.get().lastScheduleTime(sessionTime).getTime();
            Instant nextSessionTime = sr.get().nextScheduleTime(sessionTime).getTime();
            params.set("last_session_time", formatSessionTime(lastSessionTime, timeZone));
            params.set("next_session_time", formatSessionTime(nextSessionTime, timeZone));
        }
        return params;
    }
}

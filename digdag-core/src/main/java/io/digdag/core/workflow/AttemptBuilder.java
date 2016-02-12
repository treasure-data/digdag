package io.digdag.core.workflow;

import java.util.List;
import java.time.Instant;
import java.time.ZoneId;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import io.digdag.client.config.Config;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinition;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.repository.WorkflowDefinition;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.core.schedule.SlaCalculator;
import io.digdag.core.session.SessionMonitor;

public class AttemptBuilder
{
    private final SlaCalculator slaCalculator;

    @Inject
    public AttemptBuilder(SlaCalculator slaCalculator)
    {
        this.slaCalculator = slaCalculator;
    }

    public ImmutableAttemptRequest.Builder builderFromStoredWorkflow(
            StoredRevision rev, StoredWorkflowDefinition def,
            Instant runTime)
    {
        ZoneId timeZone = ScheduleExecutor.getWorkflowTimeZone(rev.getDefaultParams(), def);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(rev, def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, runTime, timeZone))
            .timeZone(timeZone);
    }

    public ImmutableAttemptRequest.Builder builderFromStoredWorkflow(
            StoredWorkflowDefinitionWithRepository def,
            Instant runTime)
    {
        ZoneId timeZone = ScheduleExecutor.getWorkflowTimeZone(def.getRevisionDefaultParams(), def);
        return ImmutableAttemptRequest.builder()
            .stored(AttemptRequest.Stored.of(def))
            .workflowName(def.getName())
            .sessionMonitors(buildSessionMonitors(def, runTime, timeZone))
            .timeZone(timeZone);
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
}

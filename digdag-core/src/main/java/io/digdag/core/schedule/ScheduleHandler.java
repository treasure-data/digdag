package io.digdag.core.schedule;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.spi.ScheduleTime;
import io.digdag.core.session.SessionOptions;
import io.digdag.spi.config.Config;
import io.digdag.spi.config.ConfigException;
import io.digdag.spi.config.ConfigFactory;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.repository.WorkflowSource;
import io.digdag.core.session.Session;
import io.digdag.core.session.SessionRelation;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.SessionMonitor;
import io.digdag.core.workflow.TaskMatchPattern;
import io.digdag.core.workflow.SubtaskMatchPattern;
import io.digdag.core.workflow.WorkflowExecutor;

public class ScheduleHandler
{
    private final ConfigFactory cf;
    private final RepositoryStoreManager rm;
    private final WorkflowExecutor exec;

    @Inject
    public ScheduleHandler(
            ConfigFactory cf,
            RepositoryStoreManager rm,
            WorkflowExecutor exec)
    {
        this.cf = cf;
        this.rm = rm;
        this.exec = exec;
    }

    public StoredSession start(int workflowId, Optional<SubtaskMatchPattern> subtaskMatchPattern,
            List<SessionMonitor> monitors, TimeZone timeZone, ScheduleTime time)
            throws ResourceNotFoundException, ResourceConflictException
    {
        StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

        Session trigger = createScheduleSession(cf, wf.getRevisionDefaultParams(), wf, subtaskMatchPattern, timeZone, time.getScheduleTime());

        SessionRelation rel = SessionRelation.ofWorkflow(wf.getRepository().getId(), wf.getRevisionId(), wf.getId());

        try {
            return exec.submitWorkflow(wf.getRepository().getSiteId(),
                    wf, subtaskMatchPattern, trigger, Optional.of(rel),
                    monitors);
        }
        catch (TaskMatchPattern.NoMatchException | TaskMatchPattern.MultipleTaskMatchException ex) {
            throw new ConfigException(ex);
        }
    }

    private static Session createScheduleSession(ConfigFactory cf,
            Config revisionDefaultParams, WorkflowSource workflow,
            Optional<SubtaskMatchPattern> subtaskMatchPattern, TimeZone timeZone, Date scheduleTime)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
        df.setTimeZone(timeZone);

        String sessionName = df.format(scheduleTime);
        if (subtaskMatchPattern.isPresent()) {
            sessionName += " " + subtaskMatchPattern.get().getPattern();
        }

        Config overwriteParams = cf.create()
            .set("schedule_time", scheduleTime.getTime() / 1000);

        return Session.sessionBuilder(
                sessionName,
                revisionDefaultParams,
                workflow,
                overwriteParams)
            .options(SessionOptions.empty())
            .build();
    }
}

package io.digdag.core.schedule;

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
import io.digdag.core.session.Sessions;
import io.digdag.core.session.SessionRelation;
import io.digdag.core.session.StoredSession;
import io.digdag.core.session.SessionMonitorManager;
import io.digdag.core.session.TaskMatchPattern;
import io.digdag.core.workflow.WorkflowExecutor;

public class ScheduleStarter
{
    private final ConfigFactory cf;
    private final RepositoryStoreManager rm;
    private final WorkflowExecutor exec;

    @Inject
    public ScheduleStarter(
            ConfigFactory cf,
            RepositoryStoreManager rm,
            WorkflowExecutor exec)
    {
        this.cf = cf;
        this.rm = rm;
        this.exec = exec;
    }

    public StoredSession start(int workflowId, Optional<String> from,
            TimeZone timeZone, ScheduleTime time)
            throws ResourceNotFoundException, ResourceConflictException
    {
        StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

        Session trigger = createScheduleSession(cf, wf, from, timeZone, time.getScheduleTime());

        SessionRelation rel = SessionRelation.ofWorkflow(wf.getRepository().getId(), wf.getRevisionId(), wf.getId());

        try {
            return exec.submitWorkflow(wf.getRepository().getSiteId(), wf, trigger, Optional.of(rel),
                    time.getRunTime(), from.transform(name -> new TaskMatchPattern(name)));
        }
        catch (TaskMatchPattern.NoMatchException | TaskMatchPattern.MultipleMatchException ex) {
            throw new ConfigException(ex);
        }
    }

    private static Session createScheduleSession(ConfigFactory cf,
            WorkflowSource workflow,
            Optional<String> from, TimeZone timeZone, Date scheduleTime)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
        df.setTimeZone(timeZone);

        String sessionName = df.format(scheduleTime);
        if (from.isPresent()) {
            sessionName += " " + from.get();
        }

        Config overwriteParams = cf.create()
            .set("schedule_time", scheduleTime.getTime() / 1000);

        return Sessions.newSession(
                sessionName,
                cf.create(),  // TODO set revision.params here
                workflow,
                overwriteParams)
            .options(SessionOptions.empty())
            .build();
    }
}

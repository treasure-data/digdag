package io.digdag.core.schedule;

import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.inject.Inject;
import com.google.common.base.Optional;
import io.digdag.core.spi.ScheduleTime;
import io.digdag.core.workflow.SessionOptions;
import io.digdag.core.config.Config;
import io.digdag.core.config.ConfigException;
import io.digdag.core.config.ConfigFactory;
import io.digdag.core.repository.RepositoryStoreManager;
import io.digdag.core.repository.StoredWorkflowSourceWithRepository;
import io.digdag.core.repository.ResourceConflictException;
import io.digdag.core.repository.ResourceNotFoundException;
import io.digdag.core.workflow.Session;
import io.digdag.core.workflow.SessionRelation;
import io.digdag.core.workflow.StoredSession;
import io.digdag.core.workflow.WorkflowExecutor;
import io.digdag.core.workflow.TaskMatchPattern;

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

        Session trigger = createScheduleSession(cf, from, timeZone, time.getScheduleTime());

        SessionRelation rel = SessionRelation.ofWorkflow(wf.getRepository().getSiteId(), wf.getRepository().getId(), wf.getId());

        try {
            return exec.submitWorkflow(wf, trigger, rel, time.getRunTime(),
                    from.transform(name -> new TaskMatchPattern(name)));
        }
        catch (TaskMatchPattern.NoMatchException | TaskMatchPattern.MultipleMatchException ex) {
            throw new ConfigException(ex);
        }
    }

    private static Session createScheduleSession(ConfigFactory cf,
            Optional<String> from, TimeZone timeZone, Date scheduleTime)
    {
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
        df.setTimeZone(timeZone);

        String sessionName = df.format(scheduleTime);
        if (from.isPresent()) {
            sessionName += " " + from.get();
        }

        Config sessionParams = cf.create()
            .set("schedule_time", scheduleTime.getTime() / 1000)
            .set("timezone", timeZone.getID());
            //.set("time_zone_offset", /*how to calculate using TimeZone API? needs joda-time?*/)

        return Session.sessionBuilder()
            .name(sessionName)
            .params(sessionParams)
            .options(SessionOptions.empty())
            .build();
    }
}

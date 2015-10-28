package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.inject.Inject;

public class StandardScheduleStarter
        implements ScheduleStarter
{
    private final ConfigSourceFactory cf;
    private final RepositoryStoreManager rm;
    private final SessionExecutor exec;

    @Inject
    public StandardScheduleStarter(
            ConfigSourceFactory cf,
            RepositoryStoreManager rm,
            SessionExecutor exec)
    {
        this.cf = cf;
        this.rm = rm;
        this.exec = exec;
    }

    public void start(int workflowId, Date scheduleTime)
    {
        TimeZone tz = TimeZone.getTimeZone("UTC");  // TODO get time zone fron workflow config
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z");
        df.setTimeZone(tz);
        String sessionName = df.format(scheduleTime);

        StoredWorkflowSourceWithRepository wf = rm.getWorkflowDetailsById(workflowId);

        ConfigSource sessionParams = cf.create()
            .set("scheduled_time", scheduleTime.getTime() / 1000)
            .set("time_zone", tz.toString());
            //.set("time_zone_offset", /*how to calculate using TimeZone API? needs joda-time?*/)

        final Session trigger = Session.sessionBuilder()
            .name(sessionName)
            .params(sessionParams)
            .options(SessionOptions.empty())
            .build();

        exec.submitWorkflow(
                wf.getRepository().getSiteId(),
                wf,
                trigger,
                SessionNamespace.ofWorkflow(wf.getRepository().getId(), wf.getId()));
    }
}

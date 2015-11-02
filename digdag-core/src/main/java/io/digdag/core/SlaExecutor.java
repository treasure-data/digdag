package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;
import java.util.TimeZone;
import java.text.SimpleDateFormat;
import com.google.common.base.*;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlaExecutor
{
    private static Logger logger = LoggerFactory.getLogger(PyTaskExecutorFactory.class);

    private final ConfigSourceFactory cf;
    private final SchedulerManager scheds;
    private final ScheduleStoreManager sm;
    private final RepositoryStoreManager rm;
    private final SessionExecutor exec;

    @Inject
    public SlaExecutor(
            ConfigSourceFactory cf,
            SchedulerManager scheds,
            ScheduleStoreManager sm,
            RepositoryStoreManager rm,
            SessionExecutor exec)
    {
        this.cf = cf;
        this.scheds = scheds;
        this.sm = sm;
        this.rm = rm;
        this.exec = exec;
    }

    public List<Schedule> getSlaTriggerSchedules(StoredWorkflowSource wf, ConfigSource schedulerConfig, ScheduleTime firstWorkflowTime)
    {
        // TODO support list
        ConfigSource config = wf.getConfig().getNestedOrGetEmpty("sla");
        if (config.isEmpty()) {
            return ImmutableList.of();
        }

        Sla sla = getSla(wf.getId(), config);
        Scheduler sr = scheds.getScheduler(schedulerConfig);
        ScheduleTime triggerTime = nextSlaScheduleTimeFromWorkflowTime(sla, sr, firstWorkflowTime);

        SlaSchedule slaSched = SlaSchedule.builder()
            .sla(sla)
            .scheduler(schedulerConfig)
            .build();
        ConfigSource slaData = cf.create(slaSched);

        Schedule trigger = Schedule.ofSla(
                wf.getId(),
                slaData,
                triggerTime.getRunTime(), triggerTime.getScheduleTime());

        return ImmutableList.of(trigger);
    }

    private static Sla getSla(int workflowId, ConfigSource config)
    {
        // TODO improve parse logic
        Optional<Integer> hour;
        Optional<Integer> minute;
        try {
            int seconds = config.get("time", Integer.class);
            hour = Optional.of(seconds / 60 / 60);
            minute = Optional.of(seconds / 60 % 60);
        }
        catch (RuntimeException ex) {
            String time = config.get("time", String.class);
            String[] hm = time.split(":", 2);
            hour = Optional.of(Integer.parseInt(hm[0]));
            minute = Optional.of(Integer.parseInt(hm[1]));
        }

        return Sla.builder()
            .workflowId(workflowId)
            .hour(hour)
            .minute(minute)
            .task(config)  // TODO take only keys starting with "+"
            .build();
    }

    public ScheduleTime triggerSla(Schedule schedule)
    {
        logger.debug("Triggering SLA scheduled as {}", schedule);

        ConfigSource slaData = schedule.getConfig();

        SlaSchedule slaSched = slaData.convert(SlaSchedule.class);
        Sla sla = slaSched.getSla();
        Scheduler sr = scheds.getScheduler(slaSched.getScheduler());

        // calculate next trigger time
        Date thisWorkflowScheduleTime = schedule.getNextScheduleTime();
        ScheduleTime nextSlaScheduleTime = nextSlaScheduleTimeFromWorkflowTime(sla, sr, sr.nextScheduleTime(thisWorkflowScheduleTime));

        // submit SLA tasks to SessionExecutor if necessary
        StoredWorkflowSourceWithRepository workflow = rm.getWorkflowDetailsById(sla.getWorkflowId());
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss Z", Locale.ENGLISH);
        df.setTimeZone(sr.getTimeZone());
        String sessionName = "SLA " + df.format(thisWorkflowScheduleTime);  // TODO include index once multiple SLAs per workflow is supported
        Session session = Session.sessionBuilder()
            .name(sessionName)
            .params(cf.create())  // TODO
            .options(SessionOptions.empty())
            .build();
        exec.submitWorkflow(sla.getSiteId(),
                WorkflowSource.of("sla", sla.getTask()),
                session,
                SessionNamespace.ofWorkflow(workflow.getRepository().getId(), workflow.getId()));

        return nextSlaScheduleTime;
    }

    private ScheduleTime nextSlaScheduleTimeFromWorkflowTime(Sla sla, Scheduler sr, ScheduleTime workflowTime)
    {
        Date sourceTime = workflowTime.getRunTime();

        Calendar cal = Calendar.getInstance(sr.getTimeZone(), Locale.ENGLISH);
        cal.setTime(sourceTime);

        Date triggerTime = overwriteTime(cal, sla).getTime();
        if (triggerTime.getTime() <= sourceTime.getTime()) {
            return nextSlaScheduleTimeFromWorkflowTime(sla, sr, sr.nextScheduleTime(workflowTime.getScheduleTime()));
        }
        return ScheduleTime.of(triggerTime, sr.nextScheduleTime(workflowTime.getScheduleTime()).getScheduleTime());
    }

    private Calendar overwriteTime(Calendar cal, Sla sla)
    {
        if (sla.getHour().isPresent()) {
            cal.set(Calendar.HOUR_OF_DAY, sla.getHour().get());
        }
        if (sla.getMinute().isPresent()) {
            cal.set(Calendar.MINUTE, sla.getMinute().get());
        }
        return cal;
    }
}

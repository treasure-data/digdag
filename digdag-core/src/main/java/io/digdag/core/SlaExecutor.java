package io.digdag.core;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;
import java.util.TimeZone;
import com.google.common.base.*;
import com.google.inject.Inject;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SlaExecutor
{
    private static Logger logger = LoggerFactory.getLogger(PyTaskExecutorFactory.class);

    private final SchedulerManager scheds;
    private final ScheduleStoreManager sm;
    private final RepositoryStoreManager rm;
    private final SessionExecutor exec;

    @Inject
    public SlaExecutor(
            SchedulerManager scheds,
            ScheduleStoreManager sm,
            RepositoryStoreManager rm,
            SessionExecutor exec)
    {
        this.scheds = scheds;
        this.sm = sm;
        this.rm = rm;
        this.exec = exec;
    }

    public List<Schedule> getSlaTriggerSchedules(StoredWorkflowSource wf, ConfigSource schedulerConfig, ScheduleTime firstWorkflowTime)
    {
        ConfigSource config = wf.getConfig().getNestedOrGetEmpty("sla");
        if (config.isEmpty()) {
            return ImmutableList.of();
        }

        Sla sla = getSla(config);
        Scheduler sr = scheds.getScheduler(schedulerConfig);
        ScheduleTime triggerTime = nextSlaScheduleTimeFromWorkflowTime(sla, sr, firstWorkflowTime);

        SlaSchedule slaSched = SlaSchedule.builder()
            .sla(sla)
            .scheduler(schedulerConfig)
            .build();
        ConfigSource slaData = schedulerConfig.getFactory().create(slaSched);

        Schedule trigger = Schedule.ofSla(
                wf.getId(),
                slaData,
                triggerTime.getRunTime(), triggerTime.getScheduleTime());

        return ImmutableList.of(trigger);
    }

    private static Sla getSla(ConfigSource config)
    {
        // TODO improve parse logic
        String time = config.get("time", String.class);
        String[] hm = time.split(":", 2);
        Optional<Integer> hour = Optional.of(Integer.parseInt(hm[0]));
        Optional<Integer> minute = Optional.of(Integer.parseInt(hm[1]));

        return Sla.builder()
            .hour(hour)
            .minute(minute)
            .build();
    }

    public ScheduleTime slaTrigger(Schedule schedule)
    {
        logger.debug("Triggering SLA scheduled as {}", schedule);

        // TODO submit SLA tasks to SessionExecutor

        ConfigSource slaData = schedule.getConfig();
        SlaSchedule slaSched = slaData.convert(SlaSchedule.class);
        Scheduler sr = scheds.getScheduler(slaSched.getScheduler());
        Date lastWorkflowScheduleTime = schedule.getNextScheduleTime();
        return nextSlaScheduleTimeFromWorkflowTime(slaSched.getSla(), sr, sr.nextScheduleTime(lastWorkflowScheduleTime));
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

package io.digdag.core.session;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;
import java.util.TimeZone;

import com.google.inject.Inject;
import com.google.common.base.*;
import com.google.common.collect.*;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.digdag.core.schedule.SchedulerManager;
import io.digdag.core.repository.WorkflowSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.digdag.spi.config.Config;

public class SessionMonitorManager
{
    private final SchedulerManager scheds;

    @Inject
    public SessionMonitorManager(SchedulerManager scheds)
    {
        this.scheds = scheds;
    }

    public List<SessionMonitor> getMonitors(WorkflowSource workflow, Date currentTime)
    {
        // TODO scheduleTime
        if (workflow.getConfig().has("sla")) {
            // TODO support multiple SLAs
            TimeZone timeZone = scheds.getSchedulerConfig(workflow)
                .transform(config -> scheds.getScheduler(config).getTimeZone())
                .or(TimeZone.getTimeZone("UTC"));
            Config slaConfig = workflow.getConfig().getNestedOrGetEmpty("sla");
            TimeCalculator calc = getCalculator(slaConfig);  // validate
            // TODO validate workflow
            Date triggerTime = calc.calculateTime(currentTime, timeZone);
            return ImmutableList.of(SessionMonitor.of(slaConfig, triggerTime));
        }
        return ImmutableList.of();
    }

    private TimeCalculator getCalculator(Config slaConfig)
    {
        // TODO improve parse logic
        int hour;
        int minute;
        try {
            int seconds = slaConfig.get("time", Integer.class);
            hour = seconds / 60 / 60;
            minute = seconds / 60 % 60;
        }
        catch (RuntimeException ex) {
            String time = slaConfig.get("time", String.class);
            String[] hm = time.split(":", 2);
            hour = Integer.parseInt(hm[0]);
            minute = Integer.parseInt(hm[1]);
        }
        return new TimeCalculator(hour, minute);
    }

    static class TimeCalculator
    {
        private final int hour;
        private final int minute;

        private TimeCalculator(int hour, int minute)
        {
            this.hour = hour;
            this.minute = minute;
        }

        public Date calculateTime(Date time, TimeZone timeZone)
        {
            Calendar cal = Calendar.getInstance(timeZone, Locale.ENGLISH);
            cal.setTime(time);
            cal.set(Calendar.HOUR_OF_DAY, hour);
            cal.set(Calendar.MINUTE, minute);
            if (cal.getTime().getTime() < time.getTime()) {
                cal.add(Calendar.DATE, 1);
            }
            return cal.getTime();
        }
    }
}

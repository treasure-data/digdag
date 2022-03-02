package io.digdag.standards.scheduler;

import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.Optional;
import java.util.TimeZone;

import com.cronutils.model.Cron;
import com.cronutils.model.CronType;
import com.cronutils.model.definition.CronDefinition;
import com.cronutils.model.definition.CronDefinitionBuilder;
import com.cronutils.model.time.ExecutionTime;
import com.cronutils.parser.CronParser;

import io.digdag.spi.ScheduleTime;
import io.digdag.spi.Scheduler;
import it.sauronsoftware.cron4j.Predictor;
import it.sauronsoftware.cron4j.SchedulingPattern;

public class QuartzCronScheduler
        implements Scheduler
{
    private final CronParser parser;
    private final ZoneId timeZone;
    private final long delaySeconds;
    private Cron SpringCronExpression;
    private ExecutionTime executionTime;

    QuartzCronScheduler(String cronPattern, ZoneId timeZone, long delaySeconds)
    {
        this.timeZone = timeZone;
        this.delaySeconds = delaySeconds;

        // new implementation
        CronDefinition cronDefinition = CronDefinitionBuilder.instanceDefinitionFor(CronType.QUARTZ);
        this.parser = new CronParser(cronDefinition);
        this.SpringCronExpression = parser.parse(cronPattern);
        this.executionTime = ExecutionTime.forCron(SpringCronExpression);
    }

    @Override
    public ZoneId getTimeZone()
    {
        return timeZone;
    }

    @Override
    public ScheduleTime getFirstScheduleTime(Instant currentTime)
    {
        ZonedDateTime startTime = currentTime.atZone(this.timeZone);  // TODO make this from config
        startTime.minusSeconds(1);
        // truncate to seconds
        ZonedDateTime lastTime = startTime.minusSeconds(1).minusSeconds(delaySeconds);

        // new implementation
        ZonedDateTime next = executionTime.nextExecution(lastTime).get();
        Instant nextTime = next.toInstant();
        System.out.println(String.format("Current time '%s' and next time '%s'",startTime, nextTime));
        return ScheduleTime.of(nextTime, nextTime.plusSeconds(delaySeconds));
    }

    @Override
    public ScheduleTime nextScheduleTime(Instant lastScheduleTime)
    {
        ZonedDateTime next = executionTime.nextExecution(lastScheduleTime.atZone(this.timeZone)).get();
        Instant nextTime = next.toInstant();
        return ScheduleTime.of(nextTime, nextTime.plusSeconds(delaySeconds));
    }

    @Override
    public ScheduleTime lastScheduleTime(Instant currentScheduleTime)
    {
        // estimate interval (doesn't have to be exact value)
        ZonedDateTime last = executionTime.lastExecution(currentScheduleTime.atZone(this.timeZone)).get();
        Instant lastTime = last.toInstant();

        // nextOfBefore is same with currentScheduleTime or after currentScheduleTime. nextOfBefore is next of before. done.
        return ScheduleTime.of(lastTime, lastTime.plusSeconds(delaySeconds));
    }

}

package io.digdag.core.workflow;

import java.time.Duration;
import java.util.List;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.time.ZoneId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;

public class SlaCalculator
{
    @Inject
    public SlaCalculator()
    { }

    public Instant getTriggerTime(Config slaConfig, Instant currentTime, ZoneId timeZone)
    {
        TriggerCalculator calc = getCalculator(slaConfig);
        return calc.calculateTime(currentTime, timeZone);
    }

    private TriggerCalculator getCalculator(Config slaConfig)
    {
        Optional<String> time = slaConfig.getOptional("time", String.class);
        Optional<String> duration = slaConfig.getOptional("duration", String.class);

        if (time.isPresent() == duration.isPresent()) {
            throw new ConfigException("SLA must be specified using either the 'time' or 'duration' option");
        }

        String option = time.isPresent() ? "time" : "duration";
        String value = time.isPresent() ? time.get() : duration.get();

        String[] fragments = value.split(":");

        Integer hour;
        Integer minute;
        Integer second;
        try {
            switch (fragments.length) {
            case 3:
                hour = Integer.parseInt(fragments[0]);
                minute = Integer.parseInt(fragments[1]);
                second = Integer.parseInt(fragments[2]);
                break;
            case 2:
                hour = Integer.parseInt(fragments[0]);
                minute = Integer.parseInt(fragments[1]);
                second = 0;
                break;
            default:
                throw new ConfigException("SLA " + option + " option needs to be HH:MM or HH:MM:SS format: " + time);
            }
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("SLA " + option + " option needs to be HH:MM or HH:MM:SS format: " + time);
        }

        if (time.isPresent()) {
            return new TimeCalculator(hour, minute, second);
        } else {
            return new DurationCalculator(hour, minute, second);
        }
    }

    private interface TriggerCalculator {
        Instant calculateTime(Instant time, ZoneId timeZone);
    }

    private static class TimeCalculator implements TriggerCalculator
    {
        private final Integer hour;
        private final Integer minute;
        private final Integer second;

        private TimeCalculator(Integer hour, Integer minute, Integer second)
        {
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public Instant calculateTime(Instant time, ZoneId timeZone)
        {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone), ENGLISH);

            cal.setTime(Date.from(time));
            if (hour != null) {
                cal.set(Calendar.HOUR_OF_DAY, hour);
            }
            if (minute != null) {
                cal.set(Calendar.MINUTE, minute);
            }
            if (second != null) {
                cal.set(Calendar.SECOND, second);
            }

            // if the time is already passed, subtract 1 day
            // TODO this assumes daily SLA
            Instant result = cal.getTime().toInstant();
            if (result.isBefore(time)) {
                result = result.plus(1, ChronoUnit.DAYS);
            }

            return result;
        }
    }

    private static class DurationCalculator implements TriggerCalculator
    {

        private final Integer hour;
        private final Integer minute;
        private final Integer second;

        private DurationCalculator(Integer hour, Integer minute, Integer second)
        {
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public Instant calculateTime(Instant time, ZoneId timeZone)
        {
            Duration duration = Duration.ofHours(hour).plusMinutes(minute).plusSeconds(second);
            Instant deadline = time.plus(duration);
            return deadline;
        }
    }
}

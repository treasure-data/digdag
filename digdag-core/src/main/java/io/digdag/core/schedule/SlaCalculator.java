package io.digdag.core.schedule;

import java.util.List;
import java.util.Date;
import java.util.TimeZone;
import java.util.Calendar;
import java.time.ZoneId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
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
        TimeCalculator calc = getCalculator(slaConfig);
        return calc.calculateTime(currentTime, timeZone);
    }

    private TimeCalculator getCalculator(Config slaConfig)
    {
        String time = slaConfig.get("time", String.class);
        String[] fragments = time.split(":");

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
                throw new ConfigException("SLA time option needs to be HH:MM or HH:MM:SS format: " + time);
            }
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("SLA time option needs to be HH:MM or HH:MM:SS format: " + time);
        }

        return new TimeCalculator(hour, minute, second);
    }

    static class TimeCalculator
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

            // if the time is already passed, subtract 1 day
            // TODO this assumes daily SLA
            Instant result = cal.getTime().toInstant();
            if (result.isBefore(time)) {
                result = result.plus(1, ChronoUnit.DAYS);
            }

            System.out.println("will trigger SLA at :" + result);
            return result;
        }
    }
}

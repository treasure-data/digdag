package io.digdag.core.schedule;

import java.util.List;
import java.util.Date;
import java.util.Locale;
import java.util.Calendar;
import java.util.TimeZone;
import io.digdag.spi.config.Config;

class SlaCalculator
{
    public static Date getTriggerTime(Config slaConfig, Date currentTime, TimeZone timeZone)
    {
        TimeCalculator calc = getCalculator(slaConfig);
        return calc.calculateTime(currentTime, timeZone);
    }

    private static TimeCalculator getCalculator(Config slaConfig)
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

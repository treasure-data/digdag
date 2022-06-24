package io.digdag.standards.scheduler;

import com.google.common.base.Optional;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.Locale;


public class ScheduleConfigHelper
{
    private static final Logger logger = LoggerFactory.getLogger(ScheduleConfigHelper.class);

    private DateTimeFormatter getFormatter()
    {
        return DateTimeFormatter
            .ofPattern("uuuu-MM-dd", Locale.ENGLISH) //Not use 'yyyy'. strict mode requires 'uuuu'
            .withResolverStyle(ResolverStyle.STRICT);       //Check non existent day.

    }

    private LocalDate getLocalDate(String ymd)
    {
        return LocalDate.from(getFormatter().parse(ymd));
    }

    public Optional<Instant> getDateTimeStart(Config config, String key, ZoneId zoneId)
        throws ConfigException
    {
        Optional<String> start = config.getOptional(key, String.class);
        try {
            Optional<Instant> dt = start.transform(v -> {
                LocalDateTime ldt = getLocalDate(v).atStartOfDay();
                ZonedDateTime zdt = ZonedDateTime.of(ldt, zoneId);
                Instant it = Instant.ofEpochSecond(zdt.toEpochSecond());
                return it;
            });
            return dt;
        }
        catch(DateTimeParseException dpe) {
            throw new ConfigException(String.format("Invalid start: %s (%s)", start.or(""), dpe));
        }
    }

    public Optional<Instant> getDateTimeEnd(Config config, String key, ZoneId zoneId)
            throws ConfigException
    {
        Optional<String> end = config.getOptional(key, String.class);
        try {
            Optional<Instant> dt = end.transform(v -> {
                LocalDateTime ldt = getLocalDate(v).plusDays(1).atStartOfDay(); // add 1 day to include the end of the day
                ZonedDateTime zdt = ZonedDateTime.of(ldt, zoneId);
                Instant it = zdt.toInstant();
                return it;
            });
            return dt;
        }
        catch(DateTimeParseException dpe) {
            throw new ConfigException(String.format("Invalid end: %s (%s)", end.or(""), dpe));
        }
    }

    public void validateStartEnd(Optional<Instant> start, Optional<Instant> end)
            throws ConfigException
    {
        if (start.isPresent() && end.isPresent()) {
            Instant s = start.get();
            Instant e = end.get();
            if (s.equals(e) || s.isAfter(e)) {
                throw new ConfigException("The schedule of end is earlier than start");
            }
        }
    }
}

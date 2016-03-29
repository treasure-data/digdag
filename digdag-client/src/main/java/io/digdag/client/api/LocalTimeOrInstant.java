package io.digdag.client.api;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.DateTimeException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public class LocalTimeOrInstant
{
    @JsonCreator
    public static LocalTimeOrInstant fromString(String arg)
    {
        Instant instant;
        LocalDateTime local;
        try {
            try {
                instant = Instant.from(DateTimeFormatter.ISO_OFFSET_DATE_TIME.parse(arg));
                local = null;
            }
            catch (DateTimeParseException ex) {
                local = LocalDateTime.from(DateTimeFormatter.ISO_LOCAL_DATE_TIME.parse(arg));
                instant = null;
            }
            return new LocalTimeOrInstant(instant, local);
        }
        catch (DateTimeException ex) {
            throw new IllegalArgumentException("Invalid timestamp format: " + arg, ex);
        }
    }

    public static LocalTimeOrInstant of(Instant instant)
    {
        return new LocalTimeOrInstant(instant, null);
    }

    public static LocalTimeOrInstant of(LocalDateTime local)
    {
        return new LocalTimeOrInstant(null, local);
    }

    private final Instant instant;
    private final LocalDateTime local;

    private LocalTimeOrInstant(Instant instant, LocalDateTime local)
    {
        this.instant = instant;
        this.local = local;
    }

    public Instant toInstant(ZoneId timeZoneIfLocalTime)
    {
        if (instant == null) {
            return local.atZone(timeZoneIfLocalTime).toInstant();
        }
        else {
            return instant;
        }
    }

    @JsonValue
    public String toString()
    {
        if (instant != null) {
            return instant.toString();
        }
        else {
            return local.toString();
        }
    }

    @Override
    public int hashCode()
    {
        if (instant != null) {
            return instant.hashCode();
        }
        else {
            return local.hashCode();
        }
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof LocalTimeOrInstant)) {
            return false;
        }
        LocalTimeOrInstant t = (LocalTimeOrInstant) o;
        if (instant != null) {
            return t.instant != null && instant.equals(t.instant);
        }
        else {
            return t.local != null && local.equals(t.local);
        }
    }
}

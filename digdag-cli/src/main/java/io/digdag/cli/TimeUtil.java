package io.digdag.cli;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;

import static io.digdag.cli.SystemExitException.systemExit;
import static java.util.Locale.ENGLISH;

public class TimeUtil
{
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    private static final DateTimeFormatter INSTANT_PARSER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss Z", ENGLISH);

    private static final DateTimeFormatter LOCAL_TIME_PARSER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd[ HH:mm:ss]", ENGLISH);

    public static String formatTime(Instant instant)
    {
        return FORMATTER.withZone(ZoneId.systemDefault()).format(instant);
    }

    public static String formatTime(OffsetDateTime time)
    {
        return FORMATTER.format(time);
    }

    public static String formatTimeDiff(Instant now, Instant from)
    {
        long seconds = now.until(from, ChronoUnit.SECONDS);
        long hours = seconds / 3600;
        seconds %= 3600;
        long minutes = seconds / 60;
        seconds %= 60;
        if (hours > 0) {
            return String.format("%2dh %2dm %2ds", hours, minutes, seconds);
        }
        else if (minutes > 0) {
            return String.format("    %2dm %2ds", minutes, seconds);
        }
        else {
            return String.format("        %2ds", seconds);
        }
    }

    public static Instant parseTime(String s, String errorMessage)
            throws SystemExitException
    {
        try {
            return Instant.ofEpochSecond(Long.parseLong(s));
        }
        catch (NumberFormatException notUnixTime) {
            try {
                return Instant.from(INSTANT_PARSER.parse(s));
            }
            catch (DateTimeException ex) {
                throw systemExit(errorMessage + ": " + s);
            }
        }
    }

    public static LocalDateTime parseLocalTime(String s, String errorMessage)
            throws SystemExitException
    {
        TemporalAccessor parsed;
        try {
            parsed = LOCAL_TIME_PARSER.parse(s);
        }
        catch (DateTimeParseException ex) {
            throw systemExit(errorMessage + ": " + s);
        }
        try {
            return LocalDateTime.from(parsed);
        }
        catch (DateTimeException ex) {
            return LocalDateTime.of(LocalDate.from(parsed), LocalTime.of(0, 0, 0));
        }
    }
}

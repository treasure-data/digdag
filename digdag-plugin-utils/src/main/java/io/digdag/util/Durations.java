package io.digdag.util;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Durations
{
    private static final Pattern PATTERN =
            Pattern.compile("\\s*(?:(?<days>\\d+)\\s*d)?\\s*(?:(?<hours>\\d+)\\s*h)?\\s*(?:(?<minutes>\\d+)\\s*m)?\\s*(?:(?<seconds>\\d+)\\s*s)?\\s*",
                    Pattern.CASE_INSENSITIVE);

    public static Duration parseDuration(CharSequence text)
    {
        Matcher matcher = PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw new DateTimeParseException("Invalid duration", text, 0);
        }
        String d = matcher.group("days");
        String h = matcher.group("hours");
        String m = matcher.group("minutes");
        String s = matcher.group("seconds");
        if (d == null && h == null && m == null && s == null) {
            throw new DateTimeParseException("Invalid duration", text, 0);
        }
        return Duration
                .ofDays(d == null ? 0 : Long.parseLong(d))
                .plusHours(h == null ? 0 : Long.parseLong(h))
                .plusMinutes(m == null ? 0 : Long.parseLong(m))
                .plusSeconds(s == null ? 0 : Long.parseLong(s));
    }

    public static String formatDuration(Duration duration)
    {
        long d = duration.toDays();
        long h = duration.minusDays(d).toHours();
        long m = duration.minusDays(d).minusHours(h).toMinutes();
        long s = duration.minusDays(d).minusHours(h).minusMinutes(m).getSeconds();

        return Stream.of(
                d == 0 ? null : d + "d",
                h == 0 ? null : h + "h",
                m == 0 ? null : m + "m",
                s == 0 ? null : s + "s")
                .filter(v -> v != null)
                .collect(Collectors.joining(" "));
    }
}

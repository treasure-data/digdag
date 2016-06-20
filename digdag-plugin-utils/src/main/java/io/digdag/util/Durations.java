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
            Pattern.compile("\\s*((?<hours>\\d+)\\s*h)?\\s*((?<minutes>\\d+)\\s*m)?\\s*((?<seconds>\\d+)\\s*s)?\\s*",
                    Pattern.CASE_INSENSITIVE);

    public static Duration parseDuration(CharSequence text)
    {
        Matcher matcher = PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw new DateTimeParseException("Invalid duration", text, 0);
        }
        String h = matcher.group("hours");
        String m = matcher.group("minutes");
        String s = matcher.group("seconds");
        if (h == null && m == null && s == null) {
            throw new DateTimeParseException("Invalid duration", text, 0);
        }
        return Duration
                .ofHours(h == null ? 0 : Long.parseLong(h))
                .plusMinutes(m == null ? 0 : Long.parseLong(m))
                .plusSeconds(s == null ? 0 : Long.parseLong(s));
    }

    public static String formatDuration(Duration duration)
    {
        long h = duration.toHours();
        long m = duration.minusHours(h).toMinutes();
        long s = duration.minusHours(h).minusMinutes(m).getSeconds();

        return Stream.of(
                h == 0 ? null : h + "h",
                m == 0 ? null : m + "m",
                s == 0 ? null : s + "s")
                .filter(v -> v != null)
                .collect(Collectors.joining(" "));
    }
}

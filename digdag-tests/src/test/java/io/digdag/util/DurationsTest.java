package io.digdag.util;

import org.hamcrest.Matchers;
import org.junit.Test;

import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.util.concurrent.Callable;

import static io.digdag.util.Durations.formatDuration;
import static io.digdag.util.Durations.parseDuration;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class DurationsTest
{
    @Test
    public void testFormatDuration()
            throws Exception
    {
        assertThat(formatDuration(parseDuration("17s")), is("17s"));
        assertThat(formatDuration(parseDuration("17S")), is("17s"));
        assertThat(formatDuration(parseDuration(" 17 s ")), is("17s"));

        assertThat(formatDuration(parseDuration("13m17s")), is("13m 17s"));
        assertThat(formatDuration(parseDuration(" 13 m 17 s ")), is("13m 17s"));
        assertThat(formatDuration(parseDuration(" 13 m 17 s ")), is("13m 17s"));
        assertThat(formatDuration(parseDuration("13M17S")), is("13m 17s"));
        assertThat(formatDuration(parseDuration(" 13 M 17 S ")), is("13m 17s"));
        assertThat(formatDuration(parseDuration(" 13 m 17 s ")), is("13m 17s"));

        assertThat(formatDuration(parseDuration("21h13m17s")), is("21h 13m 17s"));
        assertThat(formatDuration(parseDuration(" 21h 13m 17s ")), is("21h 13m 17s"));
        assertThat(formatDuration(parseDuration("21H13M17S")), is("21h 13m 17s"));
        assertThat(formatDuration(parseDuration(" 21 H 13 M 17 S ")), is("21h 13m 17s"));
    }

    @Test
    public void testValidDurations()
            throws Exception
    {
        assertThat(parseDuration("17s"), is(Duration.ZERO.plusSeconds(17)));
        assertThat(parseDuration("17S"), is(Duration.ZERO.plusSeconds(17)));
        assertThat(parseDuration(" 17 s "), is(Duration.ZERO.plusSeconds(17)));
        assertThat(parseDuration("13m17s"), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 13 m 17 s "), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 13 m 17 s "), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration("13M17S"), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 13 M 17 S "), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 13 m 17 s "), is(Duration.ZERO.plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration("21h13m17s"), is(Duration.ZERO.plusHours(21).plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 21h 13m 17s "), is(Duration.ZERO.plusHours(21).plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration("21H13M17S"), is(Duration.ZERO.plusHours(21).plusMinutes(13).plusSeconds(17)));
        assertThat(parseDuration(" 21 H 13 M 17 S "), is(Duration.ZERO.plusHours(21).plusMinutes(13).plusSeconds(17)));
    }

    @Test
    public void testInvalidValidDurations()
            throws Exception
    {
        assertThrows(() -> parseDuration(""), DateTimeParseException.class);
        assertThrows(() -> parseDuration("1 7s"), DateTimeParseException.class);
        assertThrows(() -> parseDuration("17d"), DateTimeParseException.class);
        assertThrows(() -> parseDuration("-4s"), DateTimeParseException.class);
        assertThrows(() -> parseDuration("foobar"), DateTimeParseException.class);
    }

    private static <T, E extends Throwable> void assertThrows(Callable<T> callable, Class<E> exceptionClass)
    {
        try {
            callable.call();
        }
        catch (Throwable e) {
            assertThat(e, Matchers.instanceOf(exceptionClass));
            return;
        }
        fail("Expected an exception of type: " + exceptionClass);
    }
}
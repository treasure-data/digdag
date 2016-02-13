package io.digdag.core.agent;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.client.config.ConfigFactory;
import io.digdag.spi.TaskRequest;
import static java.util.Locale.ENGLISH;

public class RuntimeParams
{
    public static Config buildRuntimeParams(ConfigFactory cf, TaskRequest request)
    {
        Config params = cf.create();

        ZoneId timeZone = request.getTimeZone();
        params.set("timezone", timeZone);

        // session_*
        params.set("session_time", formatSessionTime(request.getSessionTime(), timeZone));
        setTimeParameters(params, "session_", timeZone, request.getSessionTime());

        // last_session_*
        try {
            // last_session_time is set by AttemptBuilder
            String st = request.getConfig().get("last_session_time", String.class, null);
            if (st != null) {
                Instant instant = Instant.from(TIME_FORMAT.parse(st));
                setTimeParameters(params, "last_", timeZone, instant);
            }
        }
        catch (ConfigException | DateTimeParseException ex) {
            // skip last_session_time
        }

        // next_session_*
        try {
            // next_session_time is set by AttemptBuilder
            String st = request.getConfig().get("next_session_time", String.class, null);
            if (st != null) {
                Instant instant = Instant.from(TIME_FORMAT.parse(st));
                setTimeParameters(params, "next_", timeZone, instant);
            }
        }
        catch (ConfigException | DateTimeParseException ex) {
            // skip next_session_time
        }

        // repository_*
        params.set("repository_id", request.getRepositoryId());

        params.set("retry_attempt_name", request.getRetryAttemptName().orNull());

        return params;
    }

    private static final DateTimeFormatter TIME_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssxxx", ENGLISH);

    private static final DateTimeFormatter DATE_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd", ENGLISH);

    private static final DateTimeFormatter DATE_COMPACT_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMdd", ENGLISH);

    private static final DateTimeFormatter DATETIME_FORMAT =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss", ENGLISH);

    private static final DateTimeFormatter DATETIME_COMPACT_FORMAT =
        DateTimeFormatter.ofPattern("yyyyMMddHHmmss", ENGLISH);

    private static final DateTimeFormatter TZ_OFFSET_FORMAT =
        DateTimeFormatter.ofPattern("xx", ENGLISH);

    private static void setTimeParameters(Config params, String prefix, ZoneId timeZone, Instant instant)
    {
        params.set(prefix + "date", DATE_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "date_compact", DATE_COMPACT_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "local_time", DATETIME_FORMAT.withZone(timeZone).format(instant));
        //params.set(prefix + "local_time_compact", DATETIME_COMPACT_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "tz_offset", TZ_OFFSET_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "unixtime", instant.getEpochSecond());
    }

    // also used by AttemptBuilder to set last_session_time and next_session_time
    public static String formatSessionTime(Instant instant, ZoneId timeZone)
    {
        return TIME_FORMAT.withZone(timeZone).format(instant);
    }
}

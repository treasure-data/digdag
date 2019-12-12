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
        params.set("digdag.timezone", timeZone);

        // session_*
        params.set("session_uuid", request.getSessionUuid().toString());
        params.set("digdag.session_uuid", request.getSessionUuid().toString());
        params.set("session_time", formatSessionTime(request.getSessionTime(), timeZone));
        params.set("digdag.session_time", formatSessionTime(request.getSessionTime(), timeZone));
        params.set("session_id", request.getSessionId());
        params.set("digdag.session_id", request.getSessionId());
        setTimeParameters(params, "session_", timeZone, request.getSessionTime());
        setTimeParameters(params, "digdag.session_", timeZone, request.getSessionTime());

        // last_session_*
        try {
            // last_session_time is set by AttemptBuilder
            String st = request.getConfig().get("last_session_time", String.class, null);
            if (st != null) {
                Instant instant = Instant.from(TIME_FORMAT.parse(st));
                setTimeParameters(params, "last_session_", timeZone, instant);
                setTimeParameters(params, "digdag.last_session_", timeZone, instant);
            }
        }
        catch (ConfigException | DateTimeParseException ex) {
            // skip last_session_time
        }

        // last_executed_session_*
        try {
            // last_executed_session_time is set by AttemptBuilder
            String sat = request.getConfig().get("last_executed_session_time", String.class, null);
            if (sat != null) {
                Instant instant = Instant.from(TIME_FORMAT.parse(sat));
                setTimeParameters(params, "last_executed_session_", timeZone, instant);
                setTimeParameters(params, "digdag.last_executed_session_", timeZone, instant);
            } else {
                setEmptyTimeParameters(params, "last_executed_session_", timeZone);
                setEmptyTimeParameters(params, "digdag.last_executed_session_", timeZone);
            }
        }
        catch (ConfigException | DateTimeParseException ex) {
            // skip last_executed_session_time
        }

        // next_session_*
        try {
            // next_session_time is set by AttemptBuilder
            String st = request.getConfig().get("next_session_time", String.class, null);
            if (st != null) {
                Instant instant = Instant.from(TIME_FORMAT.parse(st));
                setTimeParameters(params, "next_session_", timeZone, instant);
                setTimeParameters(params, "digdag.next_session_", timeZone, instant);
            }
        }
        catch (ConfigException | DateTimeParseException ex) {
            // skip next_session_time
        }

        // project_*
        params.set("project_id", request.getProjectId());
        params.set("digdag.project_id", request.getProjectId());

        params.set("digdag.project_name", request.getProjectName());

        params.set("digdag.workflow_name", request.getWorkflowName());
        params.set("digdag.workflow_id", request.getWorkflowDefinitionId());

        params.set("digdag.revision_id", request.getRevision());

        params.set("digdag.created_at", request.getCreatedAt());

        params.set("retry_attempt_name", request.getRetryAttemptName().orNull());
        params.set("digdag.retry_attempt_name", request.getRetryAttemptName().orNull());
        params.set("attempt_id", request.getAttemptId());
        params.set("digdag.attempt_id", request.getAttemptId());

        // task_*
        params.set("task_name", request.getTaskName());
        params.set("digdag.task_name", request.getTaskName());

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

    private static void setEmptyTimeParameters(Config params, String prefix, ZoneId timeZone)
    {
        params.set(prefix + "time", "");
        params.set(prefix + "date", "");
        params.set(prefix + "date_compact", "");
        params.set(prefix + "local_time", "");
        //params.set(prefix + "local_time_compact", ");
        params.set(prefix + "tz_offset", TZ_OFFSET_FORMAT.withZone(timeZone).format(Instant.ofEpochSecond(0)));
        params.set(prefix + "unixtime", "");
    }

    // also used by AttemptBuilder to set last_session_time and next_session_time
    public static String formatSessionTime(Instant instant, ZoneId timeZone)
    {
        return TIME_FORMAT.withZone(timeZone).format(instant);
    }
}

package io.digdag.core.workflow;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.chrono.IsoChronology;
import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.digdag.core.session.SessionAttempt;
import io.digdag.core.session.StoredSessionAttemptWithSession;
import io.digdag.core.repository.StoredRevision;
import io.digdag.core.repository.StoredWorkflowDefinitionWithRepository;
import io.digdag.core.schedule.ScheduleExecutor;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigFactory;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;

public class TaskConfig
{
    public static Config setRuntimeBuiltInParams(Config taskLocalParams,
            Optional<StoredRevision> stored,
            StoredSessionAttemptWithSession attempt)
    {
        Config params = taskLocalParams.getFactory().create();

        ZoneId timeZone;
        try {
            timeZone = ScheduleExecutor.getTaskTimeZone(taskLocalParams, stored.transform(it -> it.getDefaultParams()));
        }
        catch (ConfigException ce) {
            timeZone = ZoneId.of("UTC");
        }
        params.set("timezone", timeZone);

        // session_*
        setTimeParameters(params, "session_", timeZone, attempt.getSession().getInstant());

        // repository_*
        params.set("repository_id", attempt.getSession().getRepositoryId());

        params.set("retry_attempt_name", attempt.getRetryAttemptName().orNull());

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
        params.set(prefix + "time", TIME_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "date", DATE_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "date_compact", DATE_COMPACT_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "local_time", DATETIME_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "local_time_compact", DATETIME_COMPACT_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "tz_offset", TZ_OFFSET_FORMAT.withZone(timeZone).format(instant));
        params.set(prefix + "unixtime", instant.getEpochSecond());
    }

    public static void validateAttempt(SessionAttempt attempt)
    {
    }

    public static TaskConfig validate(Config config)
    {
        Config copy = config.deepCopy();
        Config export = copy.getNestedOrGetEmpty("export");
        copy.remove("export");
        return new TaskConfig(copy, export);
    }

    @JsonCreator
    public static TaskConfig assumeValidated(
            @JsonProperty("local") Config local,
            @JsonProperty("export") Config export)
    {
        return new TaskConfig(local, export);
    }

    private final Config local;
    private final Config export;

    private TaskConfig(Config local, Config export)
    {
        this.local = local;
        this.export = export;
    }

    @JsonProperty("local")
    public Config getLocal()
    {
        return local;
    }

    @JsonProperty("export")
    public Config getExport()
    {
        return export;
    }

    @JsonIgnore
    public Config getMerged()
    {
        return export.deepCopy().setAll(local);
    }

    @JsonIgnore
    public Config getNonValidated()
    {
        return local.deepCopy().set("export", export);
    }

    @JsonIgnore
    public Config getCheckConfig()
    {
        return getMerged().getNestedOrGetEmpty("check").deepCopy();
    }

    @JsonIgnore
    public Config getErrorConfig()
    {
        return getMerged().getNestedOrGetEmpty("error").deepCopy();
    }

    private TaskConfig validate()
    {
        getCheckConfig();
        getErrorConfig();
        return this;
    }
}

package io.digdag.core.workflow;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Date;
import java.util.Set;
import java.util.TimeZone;
import java.util.Calendar;
import java.time.ZoneId;
import java.time.Instant;
import java.time.temporal.ChronoUnit;

import com.google.common.base.Optional;
import com.google.inject.Inject;
import io.digdag.client.config.Config;
import io.digdag.client.config.ConfigException;
import io.digdag.core.agent.CheckedConfig;
import io.digdag.core.agent.EditDistance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Locale.ENGLISH;

class UnusedConfigException extends ConfigException
{
    UnusedConfigException(String message)
    {
        super(message);
    }
}

public class SlaCalculator
{
    private static final Logger logger = LoggerFactory.getLogger(SlaCalculator.class);
    private static final List<String> BUILT_IN_SLA_PARAMS = Arrays.asList("fail", "alert");

    @Inject
    public SlaCalculator()
    { }

    public Instant getTriggerTime(Config slaConfig, Instant currentTime, ZoneId timeZone)
    {
        TriggerCalculator calc = getCalculator(slaConfig, false);
        return calc.calculateTime(currentTime, timeZone);
    }

    public void validateCalculator(Config slaConfig)
    {
        getCalculator(slaConfig, true);
    }

    private TriggerCalculator getCalculator(Config config, boolean throwUnusedKeys)
    {
        Set<String> shouldBeUsedKeys = new HashSet<>(config.getKeys());
        // Track accessed keys using UsedKeysSet class
        CheckedConfig.UsedKeysSet usedKeys = new CheckedConfig.UsedKeysSet();

        Config slaConfig = new CheckedConfig(config, usedKeys);

        for(String param : BUILT_IN_SLA_PARAMS){
            usedKeys.add(param);
        }
        for (String key: shouldBeUsedKeys) {
            if (key.startsWith("+"))
                usedKeys.add(key);
        }

        Optional<String> time = slaConfig.getOptional("time", String.class);
        Optional<String> duration = slaConfig.getOptional("duration", String.class);

        if (time.isPresent() == duration.isPresent()) {
            throw new ConfigException("SLA must be specified using either the 'time' or 'duration' option");
        }

        String option = time.isPresent() ? "time" : "duration";
        String value = time.isPresent() ? time.get() : duration.get();

        String[] fragments = value.split(":");

        Integer hour;
        Integer minute;
        Integer second;
        try {
            switch (fragments.length) {
            case 3:
                hour = Integer.parseInt(fragments[0]);
                minute = Integer.parseInt(fragments[1]);
                second = Integer.parseInt(fragments[2]);
                break;
            case 2:
                hour = Integer.parseInt(fragments[0]);
                minute = Integer.parseInt(fragments[1]);
                second = 0;
                break;
            default:
                throw new ConfigException("SLA " + option + " option needs to be HH:MM or HH:MM:SS format: " + time);
            }
        }
        catch (NumberFormatException ex) {
            throw new ConfigException("SLA " + option + " option needs to be HH:MM or HH:MM:SS format: " + time);
        }

        if (!usedKeys.isAllUsed()) {
            shouldBeUsedKeys.removeAll(usedKeys);
            if (!shouldBeUsedKeys.isEmpty()) {
                StringBuilder buf = new StringBuilder();
                for (String key: shouldBeUsedKeys) {
                    buf.append(getWarnUnusedKey(key, usedKeys));
                    buf.append("\n");
                }
                if (throwUnusedKeys)
                    throw new UnusedConfigException(buf.toString());
                else
                    logger.error(buf.toString());
            }
        }

        if (time.isPresent()) {
            return new TimeCalculator(hour, minute, second);
        } else {
            return new DurationCalculator(hour, minute, second);
        }
    }

    private interface TriggerCalculator {
        Instant calculateTime(Instant time, ZoneId timeZone);
    }

    private static class TimeCalculator implements TriggerCalculator
    {
        private final Integer hour;
        private final Integer minute;
        private final Integer second;

        private TimeCalculator(Integer hour, Integer minute, Integer second)
        {
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public Instant calculateTime(Instant time, ZoneId timeZone)
        {
            Calendar cal = Calendar.getInstance(TimeZone.getTimeZone(timeZone), ENGLISH);

            cal.setTime(Date.from(time));
            if (hour != null) {
                cal.set(Calendar.HOUR_OF_DAY, hour);
            }
            if (minute != null) {
                cal.set(Calendar.MINUTE, minute);
            }
            if (second != null) {
                cal.set(Calendar.SECOND, second);
            }

            // if the time is already passed, subtract 1 day
            // TODO this assumes daily SLA
            Instant result = cal.getTime().toInstant();
            if (result.isBefore(time)) {
                result = result.plus(1, ChronoUnit.DAYS);
            }

            return result;
        }
    }

    private static class DurationCalculator implements TriggerCalculator
    {

        private final Integer hour;
        private final Integer minute;
        private final Integer second;

        private DurationCalculator(Integer hour, Integer minute, Integer second)
        {
            this.hour = hour;
            this.minute = minute;
            this.second = second;
        }

        @Override
        public Instant calculateTime(Instant time, ZoneId timeZone)
        {
            Duration duration = Duration.ofHours(hour).plusMinutes(minute).plusSeconds(second);
            Instant deadline = time.plus(duration);
            return deadline;
        }
    }

    private String getWarnUnusedKey(String shouldBeUsedButNotUsedKey, Collection<String> candidateKeys)
    {
        StringBuilder buf = new StringBuilder();
        buf.append("Parameter '");
        buf.append(shouldBeUsedButNotUsedKey);
        buf.append("' is not used at sla. ");

        List<String> suggestions = EditDistance.suggest(shouldBeUsedButNotUsedKey, candidateKeys, 0.50);
        if (!suggestions.isEmpty()) {
            buf.append("> Did you mean '");
            buf.append(suggestions);
            buf.append("'?");
        }

        return buf.toString();
    }
}

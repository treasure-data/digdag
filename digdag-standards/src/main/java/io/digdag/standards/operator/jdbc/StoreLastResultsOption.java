package io.digdag.standards.operator.jdbc;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.digdag.client.config.ConfigException;
import static java.util.Locale.ENGLISH;

public enum StoreLastResultsOption
{
    FALSE(false),
    ALL(true),
    FIRST(true);

    private final boolean enabled;

    private StoreLastResultsOption(boolean enabled)
    {
        this.enabled = enabled;
    }

    public boolean isEnabled()
    {
        return enabled;
    }

    @JsonCreator
    public static StoreLastResultsOption parse(String text)
    {
        switch (text) {
        case "false":
            return FALSE;
        case "true":
        case "first":
            return FIRST;
        case "all":
            return ALL;
        default:
            throw new ConfigException("last_results must be either of \"first\" or \"all\": " + text);
        }
    }

    @JsonValue
    public String toString()
    {
        return name().toLowerCase(ENGLISH);
    }
}

package io.digdag.core.workflow;

import com.google.common.base.Optional;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.digdag.core.session.SessionAttempt;
import io.digdag.client.config.Config;

public class TaskConfig
{
    public static void validateAttempt(SessionAttempt attempt)
    {
    }

    public static TaskConfig validate(Config config)
    {
        Config copy = config.deepCopy();
        Config export = copy.getNestedOrGetEmpty("_export");
        copy.remove("_export");
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
        return export.deepCopy().merge(local);
    }

    @JsonIgnore
    public Config getNonValidated()
    {
        Config config = local.deepCopy();
        if (!export.isEmpty()) {
            config.set("_export", export);
        }
        return config;
    }

    @JsonIgnore
    public Config getCheckConfig()
    {
        return getMerged().getNestedOrGetEmpty("_check").deepCopy();
    }

    @JsonIgnore
    public Config getErrorConfig()
    {
        return getMerged().getNestedOrGetEmpty("_error").deepCopy();
    }

    private TaskConfig validate()
    {
        getCheckConfig();
        getErrorConfig();
        return this;
    }

    @Override
    public boolean equals(Object other)
    {
        if (!(other instanceof TaskConfig)) {
            return false;
        }
        TaskConfig o = (TaskConfig) other;
        return getNonValidated().equals(o.getNonValidated());
    }

    @Override
    public int hashCode()
    {
        return getNonValidated().hashCode();
    }

    @Override
    public String toString()
    {
        return getNonValidated().toString();
    }
}

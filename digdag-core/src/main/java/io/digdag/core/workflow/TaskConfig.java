package io.digdag.core.workflow;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonIgnore;
import io.digdag.spi.config.Config;

public class TaskConfig
{
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

package io.digdag.client.api;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.immutables.value.Value;
import io.digdag.client.config.Config;

@Value.Immutable
@JsonSerialize(as = ImmutableRestWorkflow.class)
@JsonDeserialize(as = ImmutableRestWorkflow.class)
public abstract class RestWorkflow
{
    public abstract int getId();

    public abstract String getName();

    public abstract IdName getRepository();

    public abstract String getRevision();

    public abstract Config getConfig();

    //public abstract IdName getSchedule();

    public static ImmutableRestWorkflow.Builder builder()
    {
        return ImmutableRestWorkflow.builder();
    }
}
